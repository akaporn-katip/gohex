// Package brokertest is the reusable contract-test suite for broker
// adapters. Every adapter — including the built-in MemoryBroker — must
// pass it, so the port's delivery semantics are pinned by tests:
//
//	func TestMyBroker(t *testing.T) {
//		brokertest.Run(t, func(t *testing.T) brokertest.PubSub {
//			return newBroker(t)
//		})
//	}
package brokertest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
)

// PubSub is the combined surface an adapter under test must provide.
type PubSub interface {
	broker.Publisher
	broker.Subscriber
}

const waitFor = 10 * time.Second

// Run exercises the broker contract. newBroker is called once per
// subtest; topics are uniquely named, so adapters may share one backend
// across subtests.
func Run(t *testing.T, newBroker func(t *testing.T) PubSub) {
	t.Helper()

	t.Run("published before subscribe is received", func(t *testing.T) {
		b, topic, ctx := setup(t, newBroker)
		mustPublish(t, b, topic, msg("m1"), msg("m2"))

		got := collect(t, ctx, b, topic, "g1", 2)
		if got[0].ID != "m1" || got[1].ID != "m2" {
			t.Fatalf("order: got %s, %s", got[0].ID, got[1].ID)
		}
	})

	t.Run("published while subscribed is received", func(t *testing.T) {
		b, topic, ctx := setup(t, newBroker)
		sink := newSink()
		go func() { _ = b.Subscribe(ctx, topic, "g1", sink.handler) }()

		mustPublish(t, b, topic, msg("m1"))
		sink.await(t, 1)
	})

	t.Run("each group receives every message", func(t *testing.T) {
		b, topic, ctx := setup(t, newBroker)
		mustPublish(t, b, topic, msg("m1"), msg("m2"))

		g1 := collect(t, ctx, b, topic, "g1", 2)
		g2 := collect(t, ctx, b, topic, "g2", 2)
		if len(g1) != 2 || len(g2) != 2 {
			t.Fatalf("g1 = %d msgs, g2 = %d msgs; want 2 and 2", len(g1), len(g2))
		}
	})

	t.Run("handler error causes redelivery, not skip", func(t *testing.T) {
		b, topic, ctx := setup(t, newBroker)
		mustPublish(t, b, topic, msg("m1"), msg("m2"))

		var mu sync.Mutex
		attempts := 0
		var got []broker.Message
		done := make(chan struct{})
		go func() {
			_ = b.Subscribe(ctx, topic, "g1", func(_ context.Context, m broker.Message) error {
				mu.Lock()
				defer mu.Unlock()
				if m.ID == "m1" && attempts < 2 {
					attempts++
					return errors.New("transient failure")
				}
				got = append(got, m)
				if len(got) == 2 {
					close(done)
				}
				return nil
			})
		}()

		select {
		case <-done:
		case <-time.After(waitFor):
			t.Fatal("timed out waiting for redelivery")
		}
		mu.Lock()
		defer mu.Unlock()
		if attempts != 2 {
			t.Errorf("attempts = %d, want 2", attempts)
		}
		if got[0].ID != "m1" || got[1].ID != "m2" {
			t.Errorf("delivery skipped or reordered: %s, %s", got[0].ID, got[1].ID)
		}
	})

	t.Run("envelope round trips", func(t *testing.T) {
		b, topic, ctx := setup(t, newBroker)
		occurred := time.Date(2026, 8, 9, 12, 0, 0, 123456789, time.UTC)
		in := broker.Message{
			ID:         "order/42#3",
			Key:        "42",
			Type:       "ordering.order_placed",
			Version:    2,
			OccurredAt: occurred,
			Payload:    json.RawMessage(`{"total_cents":5000}`),
			Metadata:   map[string]string{"traceparent": "00-abc-def-01"},
		}
		mustPublish(t, b, topic, in)

		got := collect(t, ctx, b, topic, "g1", 1)[0]
		if got.ID != in.ID || got.Key != in.Key || got.Type != in.Type || got.Version != in.Version {
			t.Errorf("envelope = %+v, want %+v", got, in)
		}
		if !got.OccurredAt.Equal(occurred) {
			t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, occurred)
		}
		if string(got.Payload) != `{"total_cents":5000}` {
			t.Errorf("Payload = %s", got.Payload)
		}
		if got.Metadata["traceparent"] != "00-abc-def-01" {
			t.Errorf("Metadata = %v", got.Metadata)
		}
	})

	t.Run("subscribe returns nil on cancel", func(t *testing.T) {
		b, topic, _ := setup(t, newBroker)
		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() {
			errc <- b.Subscribe(ctx, topic, "g1", func(context.Context, broker.Message) error { return nil })
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		select {
		case err := <-errc:
			if err != nil {
				t.Fatalf("Subscribe after cancel = %v, want nil", err)
			}
		case <-time.After(waitFor):
			t.Fatal("Subscribe did not return after cancel")
		}
	})
}

func setup(t *testing.T, newBroker func(t *testing.T) PubSub) (PubSub, string, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return newBroker(t), "brokertest-" + randomSuffix(), ctx
}

func msg(id string) broker.Message {
	return broker.Message{
		ID: id, Key: "k", Type: "test.event", Version: 1,
		OccurredAt: time.Now().UTC(), Payload: json.RawMessage(`{}`),
	}
}

func mustPublish(t *testing.T, p broker.Publisher, topic string, msgs ...broker.Message) {
	t.Helper()
	if err := p.Publish(context.Background(), topic, msgs...); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// collect subscribes with a fresh group and returns the first n messages.
func collect(t *testing.T, ctx context.Context, b PubSub, topic, group string, n int) []broker.Message {
	t.Helper()
	sink := newSink()
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = b.Subscribe(subCtx, topic, group, sink.handler) }()
	return sink.await(t, n)
}

type sink struct {
	mu   sync.Mutex
	msgs []broker.Message
}

func newSink() *sink { return &sink{} }

func (s *sink) handler(_ context.Context, m broker.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.msgs = append(s.msgs, m)
	return nil
}

func (s *sink) await(t *testing.T, n int) []broker.Message {
	t.Helper()
	deadline := time.Now().Add(waitFor)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		if len(s.msgs) >= n {
			out := append([]broker.Message(nil), s.msgs...)
			s.mu.Unlock()
			return out
		}
		s.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t.Fatalf("timed out: received %d of %d messages", len(s.msgs), n)
	return nil
}

func randomSuffix() string {
	var b [6]byte
	rand.Read(b[:]) //nolint:errcheck // never fails
	return hex.EncodeToString(b[:])
}
