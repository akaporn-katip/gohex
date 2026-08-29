package projection

import (
	"context"
	"fmt"

	"github.com/akaporn-katip/gohex/eventstore"
)

// CatchUp feeds a projection from the service's own event store, tailing
// the global sequence with a durable checkpoint — the same mechanism as
// the relay, pointed at read models instead of the broker.
type CatchUp struct {
	projection  *Projection
	store       eventstore.Store
	registry    *eventstore.Registry
	checkpoints eventstore.CheckpointStore
	cfg         Config
}

func NewCatchUp(p *Projection, store eventstore.Store, registry *eventstore.Registry,
	checkpoints eventstore.CheckpointStore, cfg Config) *CatchUp {
	return &CatchUp{
		projection:  p,
		store:       store,
		registry:    registry,
		checkpoints: checkpoints,
		cfg:         cfg.withDefaults(),
	}
}

// Run tails the store until ctx is cancelled (returns nil) or an error
// occurs (returns it; the supervisor restarts from the checkpoint, so
// the in-flight batch is re-applied — handlers must be idempotent).
func (c *CatchUp) Run(ctx context.Context) error {
	name := c.projection.storeCheckpoint()
	seq, err := c.checkpoints.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		recs, err := c.store.ReadAll(ctx, seq, c.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s: read: %w", name, err)
		}
		if len(recs) == 0 {
			if !sleepCtx(ctx, c.cfg.PollInterval) {
				return nil
			}
			continue
		}
		for _, rec := range recs {
			handler, ok := c.projection.domainHandlers[rec.EventName]
			if !ok {
				continue
			}
			event, err := c.registry.Decode(rec)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			meta := Meta{
				Stream:     rec.Stream,
				Version:    rec.Version,
				GlobalSeq:  rec.GlobalSeq,
				OccurredAt: rec.OccurredAt,
				Metadata:   rec.Metadata,
			}
			if err := handler(ctx, event, meta); err != nil {
				return fmt.Errorf("%s: handling %s at seq %d: %w", name, rec.EventName, rec.GlobalSeq, err)
			}
		}
		seq = recs[len(recs)-1].GlobalSeq
		if err := c.checkpoints.Set(ctx, name, seq); err != nil {
			return fmt.Errorf("%s: checkpoint: %w", name, err)
		}
	}
}
