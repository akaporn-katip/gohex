// Package projection maintains read models (ADR-0006): denormalized,
// query-shaped tables owned by one service, disposable and rebuildable
// from events.
//
// A projection is a named set of event handlers fed from two sequenced
// logs, both consumed catch-up style with a durable checkpoint:
//
//   - the service's OWN event store, tailed by [CatchUp] — full history,
//     no broker dependency;
//   - FOREIGN integration events, durably copied into an [Inbox] by
//     [InboxWriter] (idempotent by message ID) and tailed by
//     [InboxReader] — so rebuilds replay the inbox, never the broker,
//     and work past broker retention.
//
// Delivery to handlers is at-least-once (a restart re-applies the batch
// in flight), so handlers must be idempotent — upserts keyed by IDs, not
// blind inserts. And because the two logs have NO cross-ordering (a
// rebuild may apply a foreign fact before the own event that creates the
// row), handlers must also be commutative: update the columns you own
// (INSERT ... ON CONFLICT DO UPDATE SET ...), never overwrite whole
// rows. To rebuild: stop the runners, truncate the read tables, call
// [Reset], restart.
package projection

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
)

// Meta describes where an own-store event came from.
type Meta struct {
	Stream     eventstore.StreamID
	Version    int64
	GlobalSeq  int64
	OccurredAt time.Time
	Metadata   map[string]string
}

// Projection is a named set of handlers maintaining one read model.
// Register handlers at startup with [On] and [OnIntegration], then feed
// it with the runners.
type Projection struct {
	name            string
	domainHandlers  map[string]func(ctx context.Context, e kernel.DomainEvent, m Meta) error
	foreignHandlers map[string]func(ctx context.Context, msg broker.Message) error
}

func New(name string) *Projection {
	if name == "" {
		panic("projection: New: empty name")
	}
	return &Projection{
		name:            name,
		domainHandlers:  map[string]func(context.Context, kernel.DomainEvent, Meta) error{},
		foreignHandlers: map[string]func(context.Context, broker.Message) error{},
	}
}

// Name identifies the projection; checkpoints derive from it.
func (p *Projection) Name() string { return p.name }

// On registers the handler for own domain event E. One handler per
// event; duplicates panic (wiring bug).
func On[E kernel.DomainEvent](p *Projection, fn func(ctx context.Context, e E, m Meta) error) {
	var zero E
	name := zero.EventName()
	if _, dup := p.domainHandlers[name]; dup {
		panic(fmt.Sprintf("projection %s: duplicate handler for %q", p.name, name))
	}
	p.domainHandlers[name] = func(ctx context.Context, e kernel.DomainEvent, m Meta) error {
		return fn(ctx, e.(E), m)
	}
}

// OnIntegration registers the handler for foreign integration event E,
// matched by contract name AND version — a message of the same name but
// a different version is not handled (register each version you accept).
func OnIntegration[E broker.IntegrationEvent](p *Projection, fn func(ctx context.Context, e E, msg broker.Message) error) {
	var zero E
	key := integrationKey(zero.EventName(), zero.ContractVersion())
	if _, dup := p.foreignHandlers[key]; dup {
		panic(fmt.Sprintf("projection %s: duplicate handler for %q", p.name, key))
	}
	p.foreignHandlers[key] = func(ctx context.Context, msg broker.Message) error {
		var e E
		if err := json.Unmarshal(msg.Payload, &e); err != nil {
			return fmt.Errorf("projection %s: decoding %s (message %s): %w", p.name, key, msg.ID, err)
		}
		return fn(ctx, e, msg)
	}
}

func integrationKey(name string, version int) string {
	return name + "@v" + strconv.Itoa(version)
}

// storeCheckpoint and inboxCheckpoint name the projection's two cursors.
func (p *Projection) storeCheckpoint() string { return "projection." + p.name + ".store" }
func (p *Projection) inboxCheckpoint() string { return "projection." + p.name + ".inbox" }

// Reset zeroes both checkpoints so the runners replay from the start.
// Truncate the projection's read tables before restarting them.
func Reset(ctx context.Context, cps eventstore.CheckpointStore, p *Projection) error {
	if err := cps.Set(ctx, p.storeCheckpoint(), 0); err != nil {
		return fmt.Errorf("projection %s: reset: %w", p.name, err)
	}
	if err := cps.Set(ctx, p.inboxCheckpoint(), 0); err != nil {
		return fmt.Errorf("projection %s: reset: %w", p.name, err)
	}
	return nil
}

// Config tunes the polling runners.
type Config struct {
	// PollInterval is the idle wait between empty reads. Default 200ms.
	PollInterval time.Duration
	// BatchSize is the maximum events per read. Default 100.
	BatchSize int
}

func (c Config) withDefaults() Config {
	if c.PollInterval <= 0 {
		c.PollInterval = 200 * time.Millisecond
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	return c
}

// sleepCtx sleeps for d; false means ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
