package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/akaporn-katip/gohex/cqrs"
	"github.com/akaporn-katip/gohex/o11y"
	"github.com/akaporn-katip/gohex-example/ordering/internal/ports"
)

// NotifierConfig tunes the worker.
type NotifierConfig struct {
	// Interval is the wait between ticks. Default 5s — notifications are
	// business-paced, not latency-critical.
	Interval time.Duration
	// BatchSize is the maximum rows claimed per tick. Default 50.
	BatchSize int
}

func (c NotifierConfig) withDefaults() NotifierConfig {
	if c.Interval <= 0 {
		c.Interval = 5 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 50
	}
	return c
}

// Notifier is the canonical "work the list" worker: every tick it claims
// a batch of pending notifications and dispatches a command for each.
//
// It is the reference for trace boundaries at a durable hand-off
// (ADR-0015). Three rules, all visible below:
//
//  1. The tick gets its own span ([o11y.StartBatch]) — batch size, tick
//     duration and failures belong to the worker, not to any request.
//  2. Each row's work runs under [o11y.StartLinked]: a NEW trace linked
//     back to the request that queued the row (the metadata the
//     projection captured) and to the tick. The command, its events, and
//     everything the relay publishes downstream inherit that trace, plus
//     the gohex.origin.traceparent stamp — no handler touches a span.
//  3. Dispatch is at-least-once: a row is retired only after the command
//     succeeds, and the command's aggregate makes the repeat a no-op.
type Notifier struct {
	queue ports.NotificationQueue
	bus   *cqrs.Bus
	cfg   NotifierConfig
}

func NewNotifier(queue ports.NotificationQueue, bus *cqrs.Bus, cfg NotifierConfig) *Notifier {
	return &Notifier{queue: queue, bus: bus, cfg: cfg.withDefaults()}
}

// Run ticks until ctx is cancelled (returns nil). Transient failures are
// logged and retried on the next tick — the row stays pending.
func (n *Notifier) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return nil
		}
		pending, err := n.queue.Pending(ctx, n.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.ErrorContext(ctx, "notifier: reading the worklist", "error", err)
		}
		if len(pending) > 0 {
			n.tick(ctx, pending)
		}
		if !sleep(ctx, n.cfg.Interval) {
			return nil
		}
	}
}

// tick processes one claimed batch.
func (n *Notifier) tick(ctx context.Context, pending []ports.PendingNotification) {
	batchCtx, batch := o11y.StartBatch(ctx, "notify batch", len(pending))
	defer batch.End()

	for _, row := range pending {
		n.send(batchCtx, row)
	}
}

// send does one row's work under its own linked trace.
func (n *Notifier) send(batchCtx context.Context, row ports.PendingNotification) {
	ctx, span := o11y.StartLinked(batchCtx, "notify customer", row.Metadata)
	defer span.End()

	if err := n.bus.Dispatch(ctx, NotifyCustomer{OrderID: row.OrderID, Reason: row.Reason}); err != nil {
		// Left pending on purpose: the next tick retries it.
		slog.ErrorContext(ctx, "notifier: dispatch failed",
			"order_id", row.OrderID, "reason", row.Reason, "error", err)
		return
	}
	if err := n.queue.MarkSent(ctx, row.ID); err != nil {
		// The command already ran; the retry is harmless (Notify is a
		// no-op for a reason already recorded).
		slog.ErrorContext(ctx, "notifier: marking sent failed", "id", row.ID, "error", err)
	}
}

// sleep waits for d; false means ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
