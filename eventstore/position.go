package eventstore

import (
	"context"
	"sync"
)

// Position collects the write position of appends made further down the
// call chain — the plumbing for read-your-writes on a service's own
// views. An edge captures a Position before dispatching a command; every
// [Repository.Save] under that context records the GlobalSeq its append
// reached, and Seq then tells the edge what position its views must
// catch up to (see WaitForCheckpoint in the projection package).
//
//	ctx, pos := eventstore.CapturePosition(ctx)
//	if err := bus.Dispatch(ctx, cmd); err != nil { ... }
//	err := projection.WaitForCheckpoint(ctx, checkpoints, "billing.billing_views", pos.Seq(), ...)
//
// The position comes from the append that already happened — no dual
// write — and is purely observational. Safe for concurrent use.
type Position struct {
	mu  sync.Mutex
	seq int64
}

// Seq returns the highest GlobalSeq recorded so far, or 0 if nothing
// under the captured context has appended events.
func (p *Position) Seq() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seq
}

// record keeps the maximum: a handler may save several aggregates, and a
// retried dispatch records the attempt that actually committed last.
func (p *Position) record(seq int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq > p.seq {
		p.seq = seq
	}
}

type positionKey struct{}

// CapturePosition returns a derived context and the Position that saves
// under it will report into. Capturing again returns a fresh Position;
// the inner one shadows the outer for the derived context.
func CapturePosition(ctx context.Context) (context.Context, *Position) {
	p := &Position{}
	return context.WithValue(ctx, positionKey{}, p), p
}

// recordPosition reports seq to the Position captured on ctx, if any.
func recordPosition(ctx context.Context, seq int64) {
	if p, ok := ctx.Value(positionKey{}).(*Position); ok && seq > 0 {
		p.record(seq)
	}
}
