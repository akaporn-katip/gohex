// Package inboxtest is the reusable contract-test suite for
// projection.Inbox adapters. Every adapter — including the built-in
// MemoryInbox — must pass it, so the port's storage semantics are
// pinned by tests rather than by whichever adapter happens to be
// running:
//
//	func TestMyInbox(t *testing.T) {
//		inboxtest.Run(t, func(t *testing.T) projection.Inbox {
//			return newInbox(t) // empty
//		})
//	}
//
// The rejection subtests are the point of the suite: the in-memory
// inbox services are told to test against must refuse exactly what a
// durable adapter refuses, or green tests mean nothing (ADR-0018).
package inboxtest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/projection"
)

// Run exercises the Inbox contract. newInbox is called once per
// subtest and must return an empty inbox.
func Run(t *testing.T, newInbox func(t *testing.T) projection.Inbox) {
	t.Helper()

	t.Run("appended messages read back in arrival order", func(t *testing.T) {
		ctx := context.Background()
		inbox := newInbox(t)
		mustAppend(t, inbox, Message("payment/9#1"))
		mustAppend(t, inbox, Message("payment/9#2"))

		msgs, err := inbox.ReadAll(ctx, 0, 0)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(msgs) != 2 {
			t.Fatalf("inbox holds %d messages, want 2", len(msgs))
		}
		if msgs[0].Message.ID != "payment/9#1" || msgs[1].Message.ID != "payment/9#2" {
			t.Errorf("order: %s then %s", msgs[0].Message.ID, msgs[1].Message.ID)
		}
		if msgs[0].Seq >= msgs[1].Seq {
			t.Errorf("seq must increase: %d then %d", msgs[0].Seq, msgs[1].Seq)
		}
	})

	t.Run("a redelivered ID collapses and leaves the head alone", func(t *testing.T) {
		ctx := context.Background()
		inbox := newInbox(t)
		mustAppend(t, inbox, Message("payment/9#1"))
		before := mustHead(t, inbox)

		mustAppend(t, inbox, Message("payment/9#1"))
		after := mustHead(t, inbox)
		if after != before {
			t.Errorf("head moved on a duplicate append: %d then %d", before, after)
		}
		msgs, err := inbox.ReadAll(ctx, 0, 0)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(msgs) != 1 {
			t.Errorf("inbox holds %d messages, want 1 (dedup by ID)", len(msgs))
		}
	})

	t.Run("an empty inbox has head zero", func(t *testing.T) {
		if head := mustHead(t, newInbox(t)); head != 0 {
			t.Errorf("Head(empty) = %d, want 0", head)
		}
	})

	t.Run("head is the last stored seq", func(t *testing.T) {
		ctx := context.Background()
		inbox := newInbox(t)
		mustAppend(t, inbox, Message("payment/9#1"))
		mustAppend(t, inbox, Message("payment/9#2"))

		head := mustHead(t, inbox)
		msgs, err := inbox.ReadAll(ctx, 0, 0)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if got := msgs[len(msgs)-1].Seq; head != got {
			t.Errorf("Head = %d, last ReadAll seq = %d", head, got)
		}
		tail, err := inbox.ReadAll(ctx, head, 0)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(tail) != 0 {
			t.Errorf("ReadAll(after head) returned %d messages, want 0", len(tail))
		}
	})

	t.Run("a message nobody handles is still stored", func(t *testing.T) {
		inbox := newInbox(t)
		unknown := Message("payment/9#1")
		unknown.Type = "nobody.handles_this"
		unknown.Version = 7
		mustAppend(t, inbox, unknown)
		if head := mustHead(t, inbox); head == 0 {
			t.Error("an unhandled contract must still be stored: a projection extended later rebuilds from it")
		}
	})

	for _, tc := range Invalid() {
		t.Run("rejects "+tc.Name, func(t *testing.T) {
			ctx := context.Background()
			inbox := newInbox(t)
			err := inbox.Append(ctx, tc.Message)
			if err == nil {
				t.Fatalf("Append(%s) = nil, want an error wrapping projection.ErrInvalidMessage", tc.Name)
			}
			if !errors.Is(err, projection.ErrInvalidMessage) {
				t.Fatalf("Append(%s) = %v, want it to wrap projection.ErrInvalidMessage", tc.Name, err)
			}
			if head := mustHead(t, inbox); head != 0 {
				t.Errorf("a rejected message was stored anyway: head = %d", head)
			}
		})
	}
}

// Case is one structurally invalid envelope and the name it goes by.
type Case struct {
	Name    string
	Message broker.Message
}

// Invalid lists the envelopes every Inbox must reject — the same list
// drives the suite and any adapter test that wants to assert on it
// directly.
func Invalid() []Case {
	withID := func(mutate func(*broker.Message)) broker.Message {
		m := Message("payment/9#1")
		mutate(&m)
		return m
	}
	return []Case{
		{"an empty ID", withID(func(m *broker.Message) { m.ID = "" })},
		{"an empty type", withID(func(m *broker.Message) { m.Type = "" })},
		{"a zero version", withID(func(m *broker.Message) { m.Version = 0 })},
		{"a negative version", withID(func(m *broker.Message) { m.Version = -1 })},
		{"a payload that is not JSON", withID(func(m *broker.Message) { m.Payload = []byte("true but not really") })},
		{"a missing payload", withID(func(m *broker.Message) { m.Payload = nil })},
	}
}

// Message is a valid envelope with the given ID, for tests that need
// one without restating the contract.
func Message(id string) broker.Message {
	return broker.Message{
		ID:         id,
		Key:        "42",
		Type:       "billing.payment_captured",
		Version:    1,
		OccurredAt: time.Now().UTC().Truncate(time.Microsecond),
		Payload:    json.RawMessage(`{"order_id":"42"}`),
		Metadata:   map[string]string{"traceparent": "00-abc-def-01"},
	}
}

func mustAppend(t *testing.T, inbox projection.Inbox, msg broker.Message) {
	t.Helper()
	if err := inbox.Append(context.Background(), msg); err != nil {
		t.Fatalf("Append(%s): %v", msg.ID, err)
	}
}

func mustHead(t *testing.T, inbox projection.Inbox) int64 {
	t.Helper()
	head, err := inbox.Head(context.Background())
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	return head
}
