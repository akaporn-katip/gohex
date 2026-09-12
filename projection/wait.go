package projection

import (
	"context"
	"fmt"
	"time"

	"github.com/akaporn-katip/gohex/eventstore"
)

// defaultWaitPollInterval is deliberately much shorter than the runners'
// PollInterval: a waiting edge is latency-sensitive, and Get on a
// checkpoint is a cheap point read.
const defaultWaitPollInterval = 25 * time.Millisecond

// WaitOption tunes WaitForCheckpoint.
type WaitOption func(*waitConfig)

type waitConfig struct {
	pollInterval time.Duration
}

// WithWaitPollInterval replaces the default 25ms interval between
// checkpoint reads.
func WithWaitPollInterval(d time.Duration) WaitOption {
	return func(c *waitConfig) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// WaitForCheckpoint blocks until the named checkpoint reaches seq — the
// read-your-writes primitive for a service's own views. The edge
// captures its write position (see eventstore.CapturePosition), then
// waits for the projection's store checkpoint (Projection.StoreCheckpoint)
// to pass it before answering:
//
//	ctx, pos := eventstore.CapturePosition(c.Request.Context())
//	if err := bus.Dispatch(ctx, cmd); err != nil { ... }
//	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
//	defer cancel()
//	if err := projection.WaitForCheckpoint(waitCtx, checkpoints, views.StoreCheckpoint(), pos.Seq()); err != nil {
//		// timed out: answer 202 honestly, the write is still durable
//	}
//
// The wait is purely observational — it only reads checkpoints; the
// projection runners stay async and untouched. It is bounded by ctx:
// on cancellation or deadline the error wraps ctx.Err(). seq <= 0
// returns nil immediately (nothing was written, nothing to wait for).
//
// This works only for the service's own views: seq must come from the
// same store whose global sequence the checkpoint tracks. Inbox
// checkpoints count a different sequence and are not comparable.
func WaitForCheckpoint(ctx context.Context, checkpoints eventstore.CheckpointStore,
	name string, seq int64, opts ...WaitOption) error {
	if seq <= 0 {
		return nil
	}
	cfg := waitConfig{pollInterval: defaultWaitPollInterval}
	for _, opt := range opts {
		opt(&cfg)
	}
	for {
		cur, err := checkpoints.Get(ctx, name)
		if err != nil {
			return fmt.Errorf("wait for %s: %w", name, err)
		}
		if cur >= seq {
			return nil
		}
		if !sleepCtx(ctx, cfg.pollInterval) {
			return fmt.Errorf("wait for %s at seq %d (checkpoint at %d): %w", name, seq, cur, ctx.Err())
		}
	}
}
