package projection_test

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/projection"
)

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
