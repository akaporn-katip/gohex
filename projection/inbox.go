package projection

import (
	"context"
	"encoding/json"
	"errors"
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
	// no-op — the stored message is kept as it was first seen and the
	// inbox head does not move, so redeliveries collapse.
	//
	// Append accepts a structurally complete envelope only: non-empty
	// ID and Type, a Version of 1 or more, and a Payload that is valid
	// JSON ([ValidateMessage] is the rule). A message failing it is
	// rejected with an error wrapping [ErrInvalidMessage] — every
	// implementation rejects the same messages, so what the in-memory
	// inbox accepts is what a durable adapter accepts. Any other error
	// means the storage call failed and may be retried.
	Append(ctx context.Context, msg broker.Message) error
	// ReadAll returns up to limit stored messages with Seq > afterSeq in
	// arrival order (limit <= 0 means no limit).
	ReadAll(ctx context.Context, afterSeq int64, limit int) ([]InboxMessage, error)
	// Head returns the Seq of the last message the inbox holds — the
	// position an [InboxReader] reaches when it is fully caught up. An
	// empty inbox returns 0, matching a fresh checkpoint, so head minus
	// checkpoint is the inbox depth in messages (see the backlog gauges
	// in the o11y package, ADR-0017).
	Head(ctx context.Context) (int64, error)
}

// InboxMessage is a stored message plus its inbox position.
type InboxMessage struct {
	Seq     int64
	Message broker.Message
}

// ErrInvalidMessage marks an envelope the [Inbox] port refuses: it is
// structurally broken, so storing it can never succeed and redelivery
// cannot fix it (ADR-0018). Adapters wrap it; callers test with
// errors.Is.
var ErrInvalidMessage = errors.New("projection: invalid inbox message")

// ValidateMessage is the [Inbox] port's acceptance rule, exported so
// every adapter enforces one definition rather than discovering it at
// the storage layer. It checks the envelope's structure only: an
// unknown Type or a version no projection handles today is perfectly
// valid — a projection extended tomorrow rebuilds from the inbox and
// needs those messages to be there (ADR-0006).
func ValidateMessage(msg broker.Message) error {
	switch {
	case msg.ID == "":
		return fmt.Errorf("%w: empty ID", ErrInvalidMessage)
	case msg.Type == "":
		return fmt.Errorf("%w (message %s): empty Type", ErrInvalidMessage, msg.ID)
	case msg.Version < 1:
		return fmt.Errorf("%w (message %s): version %d is not a contract version", ErrInvalidMessage, msg.ID, msg.Version)
	case !json.Valid(msg.Payload):
		return fmt.Errorf("%w (message %s): payload is not valid JSON", ErrInvalidMessage, msg.ID)
	}
	return nil
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
	if err := ValidateMessage(msg); err != nil {
		return err
	}
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

func (i *MemoryInbox) Head(_ context.Context) (int64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if len(i.msgs) == 0 {
		return 0, nil
	}
	return i.msgs[len(i.msgs)-1].Seq, nil
}

// InboxWriterConfig tunes an [InboxWriter].
type InboxWriterConfig struct {
	// OnRejected is invoked when a message is definitively not storable:
	// its envelope is structurally broken, so no inbox will ever accept
	// it (see [ValidateMessage]). The message is acknowledged —
	// redelivery cannot fix a definitive rejection (ADR-0012), and a
	// writer that retried it forever would stall every later fact on the
	// partition — so wire this to record a rejection fact or alert. nil
	// drops rejections silently.
	OnRejected func(ctx context.Context, msg broker.Message, err error)
}

// InboxWriter subscribes to a foreign topic and durably appends every
// message to the inbox — nothing else. Acknowledgement happens only
// after the append, so the inbox misses nothing; the broker group's
// offset is its only cursor.
//
// A structurally invalid envelope is the one exception: it is
// acknowledged without being stored and reported to
// [InboxWriterConfig.OnRejected], the same reject-vs-retry split
// cqrs.Consumer makes (ADR-0012). Every other append failure — storage
// down, context cancelled — is returned to the broker for redelivery.
type InboxWriter struct {
	subscriber broker.Subscriber
	inbox      Inbox
	topic      string
	group      string
	cfg        InboxWriterConfig
}

func NewInboxWriter(subscriber broker.Subscriber, inbox Inbox, topic, group string, cfg InboxWriterConfig) *InboxWriter {
	return &InboxWriter{subscriber: subscriber, inbox: inbox, topic: topic, group: group, cfg: cfg}
}

// Run consumes until ctx is cancelled.
func (w *InboxWriter) Run(ctx context.Context) error {
	return w.subscriber.Subscribe(ctx, w.topic, w.group, w.handle)
}

// Handler returns the broker.Handler for callers that manage their own
// subscription.
func (w *InboxWriter) Handler() broker.Handler { return w.handle }

func (w *InboxWriter) handle(ctx context.Context, msg broker.Message) error {
	if err := w.inbox.Append(ctx, msg); err != nil {
		if errors.Is(err, ErrInvalidMessage) {
			if w.cfg.OnRejected != nil {
				w.cfg.OnRejected(ctx, msg, err)
			}
			return nil // ack: no redelivery can make this message storable
		}
		return err // transient: broker redelivers
	}
	return nil
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
	name := r.projection.InboxCheckpoint()
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
			item := Item{
				Projection: r.projection.Name(),
				Source:     SourceInbox,
				Name:       m.Message.Type,
				ID:         m.Message.ID,
				Metadata:   m.Message.Metadata,
			}
			err := r.cfg.observe(ctx, item, func(ctx context.Context) error {
				return handler(ctx, m.Message)
			})
			if err != nil {
				return fmt.Errorf("%s: handling %s (message %s): %w", name, m.Message.Type, m.Message.ID, err)
			}
		}
		seq = msgs[len(msgs)-1].Seq
		if err := r.checkpoints.Set(ctx, name, seq); err != nil {
			return fmt.Errorf("%s: checkpoint: %w", name, err)
		}
	}
}
