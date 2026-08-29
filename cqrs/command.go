// Package cqrs is the application layer's dispatch machinery: a command
// bus and a query bus with middleware, plus the wire plumbing that lets
// commands travel between services as messages (ADR-0008).
//
// The bus enforces the error split of ADR-0012 mechanically: a
// kernel.DomainError is a definitive rejection and is never retried;
// any other error is treated as transient and retried. Version
// conflicts from optimistic concurrency fall in the second bucket by
// design — the handler reloads the aggregate and tries again.
package cqrs

import (
	"context"
	"fmt"
	"time"

	"github.com/akaporn-katip/gohex/kernel"
)

// Command is an instruction to change state, addressed to exactly one
// handler. Like domain events, commands carry value objects and are
// named "<context>.<imperative>", e.g. "billing.capture_payment".
type Command interface {
	CommandName() string
}

// HandlerFunc is the untyped shape middleware wraps. Application code
// registers typed handlers via [Handle] instead.
type HandlerFunc func(ctx context.Context, cmd Command) error

// Middleware wraps command handling (logging, tracing, metrics).
// Middleware runs outside the retry loop: it observes one Dispatch,
// however many attempts that takes.
type Middleware func(next HandlerFunc) HandlerFunc

// RetryPolicy governs how Dispatch retries handler errors.
type RetryPolicy struct {
	// Attempts is the total number of tries, including the first.
	// Default 3.
	Attempts int
	// Backoff is the wait between tries. Default 25ms.
	Backoff time.Duration
	// ShouldRetry classifies errors. The default retries everything
	// except a kernel.DomainError (ADR-0012): business rejections are
	// final; conflicts and infrastructure hiccups are worth retrying.
	ShouldRetry func(error) bool
}

func (p RetryPolicy) withDefaults() RetryPolicy {
	if p.Attempts <= 0 {
		p.Attempts = 3
	}
	if p.Backoff <= 0 {
		p.Backoff = 25 * time.Millisecond
	}
	if p.ShouldRetry == nil {
		p.ShouldRetry = func(err error) bool {
			_, isDomain := kernel.AsDomainError(err)
			return !isDomain
		}
	}
	return p
}

// Bus routes commands to their registered handlers. Register handlers
// and middleware at startup; Dispatch is safe for concurrent use
// afterwards.
type Bus struct {
	handlers   map[string]HandlerFunc
	middleware []Middleware
	retry      RetryPolicy
}

// Option configures a Bus.
type Option func(*Bus)

// WithMiddleware appends middleware; the first listed is outermost.
func WithMiddleware(mw ...Middleware) Option {
	return func(b *Bus) { b.middleware = append(b.middleware, mw...) }
}

// WithRetry replaces the default retry policy.
func WithRetry(p RetryPolicy) Option {
	return func(b *Bus) { b.retry = p }
}

func NewBus(opts ...Option) *Bus {
	b := &Bus{handlers: map[string]HandlerFunc{}}
	for _, opt := range opts {
		opt(b)
	}
	b.retry = b.retry.withDefaults()
	return b
}

// Handle registers the handler for command type C. One handler per
// command; a duplicate registration panics (wiring bug).
func Handle[C Command](b *Bus, fn func(ctx context.Context, cmd C) error) {
	var zero C
	name := zero.CommandName()
	if name == "" {
		panic("cqrs: Handle: empty command name")
	}
	if _, dup := b.handlers[name]; dup {
		panic(fmt.Sprintf("cqrs: Handle: duplicate handler for %q", name))
	}
	b.handlers[name] = func(ctx context.Context, cmd Command) error {
		return fn(ctx, cmd.(C))
	}
}

// Dispatch routes cmd through middleware to its handler, applying the
// retry policy. The returned error is the last attempt's error: a
// DomainError is a definitive rejection, anything else exhausted its
// retries.
func (b *Bus) Dispatch(ctx context.Context, cmd Command) error {
	h, ok := b.handlers[cmd.CommandName()]
	if !ok {
		return fmt.Errorf("cqrs: no handler for command %q", cmd.CommandName())
	}
	final := b.withRetry(h)
	for i := len(b.middleware) - 1; i >= 0; i-- {
		final = b.middleware[i](final)
	}
	return final(ctx, cmd)
}

func (b *Bus) withRetry(h HandlerFunc) HandlerFunc {
	return func(ctx context.Context, cmd Command) error {
		var err error
		for attempt := 1; ; attempt++ {
			err = h(ctx, cmd)
			if err == nil || attempt >= b.retry.Attempts || !b.retry.ShouldRetry(err) {
				return err
			}
			select {
			case <-ctx.Done():
				return err
			case <-time.After(b.retry.Backoff):
			}
		}
	}
}
