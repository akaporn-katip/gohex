// Package relay is the framework's polling relay (ADR-0003): it tails a
// service's event store by global sequence, maps domain events through
// registered translators into versioned integration events (ADR-0004),
// publishes them to the broker, and checkpoints its position.
//
// Delivery is at-least-once in commit order: the checkpoint advances
// only after a successful publish, and message IDs are deterministic
// ("category/id#version") so consumers can deduplicate redeliveries.
// Domain events with no registered translator stay private and are
// skipped (their position is still checkpointed).
package relay

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/kernel"
)

// Config configures a Relay.
type Config struct {
	// Name identifies this relay's checkpoint, e.g. "ordering.relay".
	Name string
	// Topic is the destination for every translated event, conventionally
	// "<service>.events".
	Topic string
	// PollInterval is the idle wait between empty reads. Default 200ms.
	PollInterval time.Duration
	// BatchSize is the maximum events per read. Default 100.
	BatchSize int
	// PublishBackoff is the wait between publish retries when the broker
	// is unavailable. Default 1s.
	PublishBackoff time.Duration
}

// Relay tails one event store and publishes to one topic. Configure its
// translators at startup, then call Run.
type Relay struct {
	store       eventstore.Store
	registry    *eventstore.Registry
	publisher   broker.Publisher
	checkpoints eventstore.CheckpointStore
	cfg         Config
	translators map[string]func(kernel.DomainEvent) (broker.IntegrationEvent, bool)
}

func New(store eventstore.Store, registry *eventstore.Registry, publisher broker.Publisher,
	checkpoints eventstore.CheckpointStore, cfg Config) *Relay {
	if cfg.Name == "" || cfg.Topic == "" {
		panic("relay: Config.Name and Config.Topic are required")
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 200 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.PublishBackoff <= 0 {
		cfg.PublishBackoff = time.Second
	}
	return &Relay{
		store:       store,
		registry:    registry,
		publisher:   publisher,
		checkpoints: checkpoints,
		cfg:         cfg,
		translators: map[string]func(kernel.DomainEvent) (broker.IntegrationEvent, bool){},
	}
}

// Translate registers the translator for domain event E (ADR-0004).
// Returning false keeps that occurrence private. Registering E twice
// panics — one translator per domain event.
func Translate[E kernel.DomainEvent](r *Relay, fn func(E) (broker.IntegrationEvent, bool)) {
	var zero E
	name := zero.EventName()
	if _, dup := r.translators[name]; dup {
		panic(fmt.Sprintf("relay: duplicate translator for %q", name))
	}
	r.translators[name] = func(e kernel.DomainEvent) (broker.IntegrationEvent, bool) {
		// The registry guarantees the decoded type for this name.
		return fn(e.(E))
	}
}

// Run tails the store until ctx is cancelled (returns nil) or an
// unrecoverable error occurs (returns it; the supervisor restarts from
// the checkpoint). Broker unavailability is not unrecoverable: publishes
// retry with backoff.
func (r *Relay) Run(ctx context.Context) error {
	seq, err := r.checkpoints.Get(ctx, r.cfg.Name)
	if err != nil {
		return fmt.Errorf("relay %s: %w", r.cfg.Name, err)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		recs, err := r.store.ReadAll(ctx, seq, r.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("relay %s: read: %w", r.cfg.Name, err)
		}
		if len(recs) == 0 {
			if !sleepCtx(ctx, r.cfg.PollInterval) {
				return nil
			}
			continue
		}

		msgs := make([]broker.Message, 0, len(recs))
		for _, rec := range recs {
			msg, publish, err := r.translate(rec)
			if err != nil {
				return fmt.Errorf("relay %s: %w", r.cfg.Name, err)
			}
			if publish {
				msgs = append(msgs, msg)
			}
		}
		if len(msgs) > 0 {
			if !r.publishWithRetry(ctx, msgs) {
				return nil // shut down mid-retry; checkpoint unchanged
			}
		}
		seq = recs[len(recs)-1].GlobalSeq
		if err := r.checkpoints.Set(ctx, r.cfg.Name, seq); err != nil {
			return fmt.Errorf("relay %s: checkpoint: %w", r.cfg.Name, err)
		}
	}
}

func (r *Relay) translate(rec eventstore.RecordedEvent) (broker.Message, bool, error) {
	translator, ok := r.translators[rec.EventName]
	if !ok {
		return broker.Message{}, false, nil // private by default (ADR-0004)
	}
	event, err := r.registry.Decode(rec)
	if err != nil {
		return broker.Message{}, false, err
	}
	integration, ok := translator(event)
	if !ok {
		return broker.Message{}, false, nil
	}
	id := rec.Stream.Category + "/" + rec.Stream.ID + "#" + strconv.FormatInt(rec.Version, 10)
	msg, err := broker.NewMessage(id, rec.Stream.ID, rec.OccurredAt, integration, rec.Metadata)
	if err != nil {
		return broker.Message{}, false, err
	}
	return msg, true, nil
}

// publishWithRetry retries until success; false means ctx ended first.
func (r *Relay) publishWithRetry(ctx context.Context, msgs []broker.Message) bool {
	for {
		if err := r.publisher.Publish(ctx, r.cfg.Topic, msgs...); err == nil {
			return true
		}
		if !sleepCtx(ctx, r.cfg.PublishBackoff) {
			return false
		}
	}
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
