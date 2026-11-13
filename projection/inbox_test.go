package projection_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/projection"
	"github.com/akaporn-katip/gohex/projection/inboxtest"
)

// MemoryInbox is what services are told to test against, so it answers
// to the same contract suite as every durable adapter.
func TestMemoryInboxContract(t *testing.T) {
	inboxtest.Run(t, func(t *testing.T) projection.Inbox {
		return projection.NewMemoryInbox()
	})
}

// A structurally broken message must not wedge the writer: before the
// reject-vs-retry split existed, Append failed forever, the broker
// redelivered forever, and every later fact on the partition stopped
// arriving while the pod stayed happily alive.
func TestInboxWriterAcksStructurallyInvalidMessage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var mu sync.Mutex
	var rejected []broker.Message
	var rejectedErrs []error

	memBroker := broker.NewMemoryBroker()
	inbox := projection.NewMemoryInbox()
	writer := projection.NewInboxWriter(memBroker, inbox, "billing.events", "ordering.order_summary",
		projection.InboxWriterConfig{
			OnRejected: func(_ context.Context, msg broker.Message, err error) {
				mu.Lock()
				defer mu.Unlock()
				rejected = append(rejected, msg)
				rejectedErrs = append(rejectedErrs, err)
			},
		})
	go func() { _ = writer.Run(ctx) }()

	// A hand-written record straight into the topic: no ID, no type, no
	// version, and a payload that is not JSON — exactly the shape that
	// froze a partition in production.
	poison := broker.Message{Payload: []byte("true but not really")}
	if err := memBroker.Publish(ctx, "billing.events", poison, paidMsg("payment/9#1", "42")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "the valid message behind the poison one lands", func() bool {
		msgs, err := inbox.ReadAll(ctx, 0, 0)
		return err == nil && len(msgs) == 1 && msgs[0].Message.ID == "payment/9#1"
	})

	mu.Lock()
	defer mu.Unlock()
	if len(rejected) != 1 {
		t.Fatalf("OnRejected called %d times, want 1", len(rejected))
	}
	if string(rejected[0].Payload) != string(poison.Payload) {
		t.Errorf("OnRejected saw %+v, want the poison message", rejected[0])
	}
	if !errors.Is(rejectedErrs[0], projection.ErrInvalidMessage) {
		t.Errorf("rejection error = %v, want it to wrap projection.ErrInvalidMessage", rejectedErrs[0])
	}
}

// A nil OnRejected drops the rejection silently — documented behaviour,
// and it must still not wedge the writer.
func TestInboxWriterWithoutRejectionCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	memBroker := broker.NewMemoryBroker()
	inbox := projection.NewMemoryInbox()
	writer := projection.NewInboxWriter(memBroker, inbox, "billing.events", "ordering.order_summary",
		projection.InboxWriterConfig{})
	go func() { _ = writer.Run(ctx) }()

	if err := memBroker.Publish(ctx, "billing.events",
		broker.Message{ID: "no-type", Version: 1, Payload: []byte(`{}`)},
		paidMsg("payment/9#1", "42"),
	); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "writer moves past the rejected message", func() bool {
		head, err := inbox.Head(ctx)
		return err == nil && head == 1
	})
}

func TestMemoryInboxHead(t *testing.T) {
	ctx := context.Background()

	t.Run("empty inbox is zero", func(t *testing.T) {
		head, err := projection.NewMemoryInbox().Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		if head != 0 {
			t.Errorf("Head(empty) = %d, want 0", head)
		}
	})

	t.Run("head is the last stored seq", func(t *testing.T) {
		inbox := projection.NewMemoryInbox()
		if err := inbox.Append(ctx, paidMsg("payment/9#1", "42")); err != nil {
			t.Fatal(err)
		}
		if err := inbox.Append(ctx, paidMsg("payment/9#2", "43")); err != nil {
			t.Fatal(err)
		}
		head, err := inbox.Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		msgs, err := inbox.ReadAll(ctx, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if got := msgs[len(msgs)-1].Seq; head != got {
			t.Errorf("Head = %d, last ReadAll seq = %d", head, got)
		}
		tail, err := inbox.ReadAll(ctx, head, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(tail) != 0 {
			t.Errorf("ReadAll(after head) returned %d messages, want 0", len(tail))
		}
	})

	t.Run("a collapsed redelivery leaves the head alone", func(t *testing.T) {
		inbox := projection.NewMemoryInbox()
		if err := inbox.Append(ctx, paidMsg("payment/9#1", "42")); err != nil {
			t.Fatal(err)
		}
		before, err := inbox.Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		if err := inbox.Append(ctx, paidMsg("payment/9#1", "42")); err != nil {
			t.Fatal(err)
		}
		after, err := inbox.Head(ctx)
		if err != nil {
			t.Fatalf("Head: %v", err)
		}
		if after != before {
			t.Errorf("Head moved on a duplicate append: %d then %d", before, after)
		}
	})

	t.Run("head meets the reader's checkpoint when it catches up", func(t *testing.T) {
		tb := newSummaryTable()
		inbox := projection.NewMemoryInbox()
		cps := eventstore.NewMemoryCheckpointStore()
		p := newProjection(tb)

		if err := inbox.Append(ctx, paidMsg("payment/9#1", "42")); err != nil {
			t.Fatal(err)
		}
		run(t, "InboxReader", projection.NewInboxReader(p, inbox, cps, fastCfg).Run)
		waitFor(t, "reader caught up", func() bool {
			head, err := inbox.Head(ctx)
			if err != nil {
				return false
			}
			seq, err := cps.Get(ctx, p.InboxCheckpoint())
			return err == nil && seq == head && head > 0
		})
	})
}

// The checkpoint names are durable data: rows written by deployed
// services, and the keys build tooling resets. Exporting
// InboxCheckpoint must not have renamed anything.
func TestCheckpointNames(t *testing.T) {
	p := projection.New("order_summary")
	if got := p.StoreCheckpoint(); got != "projection.order_summary.store" {
		t.Errorf("StoreCheckpoint() = %q", got)
	}
	if got := p.InboxCheckpoint(); got != "projection.order_summary.inbox" {
		t.Errorf("InboxCheckpoint() = %q", got)
	}
}
