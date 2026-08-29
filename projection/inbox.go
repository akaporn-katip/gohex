package projection

import (
	"context"
	"fmt"
	"sync"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
)

// Inbox durably stores consumed foreign integration events in arrival
// order (ADR-0006). It is the service's local copy of the facts it
// depends on: projections replay the inbox, never the broker, so
// rebuilds work past broker retention.
type Inbox interface {
	// Append stores msg if its ID is new; appending a known ID is a
	// no-op (redeliveries collapse).
	Append(ctx context.Context, msg broker.Message) error
	// ReadAll returns up to limit stored messages with Seq > afterSeq in
	// arrival order (limit <= 0 means no limit).
	ReadAll(ctx context.Context, afterSeq int64, limit int) ([]InboxMessage, error)
}

// InboxMessage is a stored message plus its inbox position.
type InboxMessage struct {
	Seq     int64
	Message broker.Message
}

// MemoryInbox is an in-process Inbox for tests and single-process wiring.
type MemoryInbox struct {
	mu   sync.Mutex
	msgs []InboxMessage
	byID map[string]bool
}

var _ Inbox = (*MemoryInbox)(nil)

func NewMemoryInbox() *MemoryInbox {
	return &MemoryInbox{byID: map[string]bool{}}
}

func (i *MemoryInbox) Append(_ context.Context, msg broker.Message) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.byID[msg.ID] {
		return nil
	}
	i.byID[msg.ID] = true
	i.msgs = append(i.msgs, InboxMessage{Seq: int64(len(i.msgs)) + 1, Message: msg})
	return nil
}

func (i *MemoryInbox) ReadAll(_ context.Context, afterSeq int64, limit int) ([]InboxMessage, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	var out []InboxMessage
	for _, m := range i.msgs {
		if m.Seq <= afterSeq {
			continue
		}
		out = append(out, m)
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

// InboxWriter subscribes to a foreign topic and durably appends every
// message to the inbox — nothing else. Acknowledgement happens only
// after the append, so the inbox misses nothing; the broker group's
// offset is its only cursor.
type InboxWriter struct {
	subscriber broker.Subscriber
	inbox      Inbox
	topic      string
	group      string
}

func NewInboxWriter(subscriber broker.Subscriber, inbox Inbox, topic, group string) *InboxWriter {
	return &InboxWriter{subscriber: subscriber, inbox: inbox, topic: topic, group: group}
}

// Run consumes until ctx is cancelled.
func (w *InboxWriter) Run(ctx context.Context) error {
	return w.subscriber.Subscribe(ctx, w.topic, w.group, func(ctx context.Context, msg broker.Message) error {
		return w.inbox.Append(ctx, msg)
	})
}

// InboxReader feeds a projection from the inbox, tailing it with a
// durable checkpoint — the exact same catch-up shape as [CatchUp],
// pointed at the inbox. Messages with no registered (name, version)
// handler are skipped and checkpointed past.
type InboxReader struct {
	projection  *Projection
	inbox       Inbox
	checkpoints eventstore.CheckpointStore
	cfg         Config
}

func NewInboxReader(p *Projection, inbox Inbox, checkpoints eventstore.CheckpointStore, cfg Config) *InboxReader {
	return &InboxReader{projection: p, inbox: inbox, checkpoints: checkpoints, cfg: cfg.withDefaults()}
}

// Run tails the inbox until ctx is cancelled (returns nil) or an error
// occurs (returns it; the supervisor restarts from the checkpoint, so
// the in-flight batch is re-applied — handlers must be idempotent).
func (r *InboxReader) Run(ctx context.Context) error {
	name := r.projection.inboxCheckpoint()
	seq, err := r.checkpoints.Get(ctx, name)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		msgs, err := r.inbox.ReadAll(ctx, seq, r.cfg.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s: read: %w", name, err)
		}
		if len(msgs) == 0 {
			if !sleepCtx(ctx, r.cfg.PollInterval) {
				return nil
			}
			continue
		}
		for _, m := range msgs {
			handler, ok := r.projection.foreignHandlers[integrationKey(m.Message.Type, m.Message.Version)]
			if !ok {
				continue
			}
			if err := handler(ctx, m.Message); err != nil {
				return fmt.Errorf("%s: handling %s (message %s): %w", name, m.Message.Type, m.Message.ID, err)
			}
		}
		seq = msgs[len(msgs)-1].Seq
		if err := r.checkpoints.Set(ctx, name, seq); err != nil {
			return fmt.Errorf("%s: checkpoint: %w", name, err)
		}
	}
}
