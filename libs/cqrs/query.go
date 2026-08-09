package cqrs

import (
	"context"
	"fmt"
)

// Query is a read request answered from a read model (ADR-0006). Queries
// never change state and are not retried by the bus.
type Query interface {
	QueryName() string
}

// QueryHandlerFunc is the untyped shape query middleware wraps.
type QueryHandlerFunc func(ctx context.Context, q Query) (any, error)

// QueryMiddleware wraps query handling.
type QueryMiddleware func(next QueryHandlerFunc) QueryHandlerFunc

// QueryBus routes queries to their registered handlers. Register at
// startup; Dispatch is safe for concurrent use afterwards.
type QueryBus struct {
	handlers   map[string]QueryHandlerFunc
	middleware []QueryMiddleware
}

func NewQueryBus(mw ...QueryMiddleware) *QueryBus {
	return &QueryBus{handlers: map[string]QueryHandlerFunc{}, middleware: mw}
}

// HandleQuery registers the handler for query type Q returning R.
// A duplicate registration panics (wiring bug).
func HandleQuery[Q Query, R any](b *QueryBus, fn func(ctx context.Context, q Q) (R, error)) {
	var zero Q
	name := zero.QueryName()
	if name == "" {
		panic("cqrs: HandleQuery: empty query name")
	}
	if _, dup := b.handlers[name]; dup {
		panic(fmt.Sprintf("cqrs: HandleQuery: duplicate handler for %q", name))
	}
	b.handlers[name] = func(ctx context.Context, q Query) (any, error) {
		return fn(ctx, q.(Q))
	}
}

// Dispatch routes q through middleware to its handler. Prefer [Ask] for
// a typed result.
func (b *QueryBus) Dispatch(ctx context.Context, q Query) (any, error) {
	h, ok := b.handlers[q.QueryName()]
	if !ok {
		return nil, fmt.Errorf("cqrs: no handler for query %q", q.QueryName())
	}
	final := h
	for i := len(b.middleware) - 1; i >= 0; i-- {
		final = b.middleware[i](final)
	}
	return final(ctx, q)
}

// Ask dispatches q and returns the typed result.
func Ask[R any](ctx context.Context, b *QueryBus, q Query) (R, error) {
	v, err := b.Dispatch(ctx, q)
	if err != nil {
		var zero R
		return zero, err
	}
	r, ok := v.(R)
	if !ok {
		var zero R
		return zero, fmt.Errorf("cqrs: query %q returned %T, caller wants %T", q.QueryName(), v, zero)
	}
	return r, nil
}
