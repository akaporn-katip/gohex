package cqrs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
)

type consumerFixture struct {
	bus      *cqrs.Bus
	broker   *broker.MemoryBroker
	mu       sync.Mutex
	handled  []capturePayment
	rejected []error
}

func newConsumerFixture(t *testing.T, handler func(context.Context, capturePayment) error) *consumerFixture {
	t.Helper()
	f := &consumerFixture{
		bus:    cqrs.NewBus(cqrs.WithRetry(cqrs.RetryPolicy{Attempts: 1})),
		broker: broker.NewMemoryBroker(),
	}
	cqrs.Handle(f.bus, func(ctx context.Context, cmd capturePayment) error {
		if err := handler(ctx, cmd); err != nil {
			return err
		}
		f.mu.Lock()
		f.handled = append(f.handled, cmd)
		f.mu.Unlock()
		return nil
	})

	registry := cqrs.NewRegistry()
	cqrs.RegisterCommand[capturePayment](registry)

	consumer := cqrs.NewConsumer(f.bus, registry, cqrs.NewMemoryDeduplicator(), cqrs.ConsumerConfig{
		OnRejected: func(_ context.Context, _ broker.Message, err error) {
			f.mu.Lock()
			f.rejected = append(f.rejected, err)
			f.mu.Unlock()
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = consumer.Run(ctx, f.broker, "billing.commands", "billing") }()
	return f
}

func (f *consumerFixture) send(t *testing.T, id string, cmd cqrs.Command) {
	t.Helper()
	msg, err := cqrs.NewCommandMessage(id, "42", time.Now().UTC(), cmd, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.broker.Publish(context.Background(), "billing.commands", msg); err != nil {
		t.Fatal(err)
	}
}

func (f *consumerFixture) waitHandled(t *testing.T, n int) []capturePayment {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		if len(f.handled) >= n {
			out := append([]capturePayment(nil), f.handled...)
			f.mu.Unlock()
			return out
		}
		f.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	t.Fatalf("timed out: handled %d of %d", len(f.handled), n)
	return nil
}

func TestConsumerExecutesCommand(t *testing.T) {
	f := newConsumerFixture(t, func(context.Context, capturePayment) error { return nil })
	f.send(t, "saga/42#2", capturePayment{OrderID: "42", Cents: 100})

	got := f.waitHandled(t, 1)
	if got[0].OrderID != "42" || got[0].Cents != 100 {
		t.Errorf("handled %+v", got[0])
	}
}

func TestConsumerDeduplicatesByID(t *testing.T) {
	f := newConsumerFixture(t, func(context.Context, capturePayment) error { return nil })
	f.send(t, "saga/42#2", capturePayment{OrderID: "42", Cents: 100})
	f.send(t, "saga/42#2", capturePayment{OrderID: "42", Cents: 100}) // redelivery
	f.send(t, "saga/42#3", capturePayment{OrderID: "42", Cents: 200}) // distinct command

	got := f.waitHandled(t, 2)
	time.Sleep(20 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.handled) != 2 {
		t.Fatalf("handled %d commands, want 2 (duplicate must not re-execute)", len(f.handled))
	}
	if got[0].Cents != 100 || got[1].Cents != 200 {
		t.Errorf("handled %+v", got)
	}
}

func TestConsumerAcksDomainErrorAndReports(t *testing.T) {
	f := newConsumerFixture(t, func(context.Context, capturePayment) error { return errDeclined })
	f.send(t, "saga/42#2", capturePayment{OrderID: "42"})
	f.send(t, "saga/42#3", capturePayment{OrderID: "43"})

	// The second command is only reachable if the first was acked, not
	// redelivered forever.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		if len(f.rejected) >= 2 {
			f.mu.Unlock()
			break
		}
		f.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rejected) != 2 {
		t.Fatalf("rejected = %d, want 2", len(f.rejected))
	}
	if !errors.Is(f.rejected[0], errDeclined) {
		t.Errorf("rejected[0] = %v", f.rejected[0])
	}
	if len(f.handled) != 0 {
		t.Errorf("handled = %d, want 0", len(f.handled))
	}
}

func TestConsumerRedeliversTransientErrors(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	f := newConsumerFixture(t, func(context.Context, capturePayment) error {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		if attempts < 3 {
			return errors.New("db down")
		}
		return nil
	})
	f.send(t, "saga/42#2", capturePayment{OrderID: "42"})

	f.waitHandled(t, 1)
	mu.Lock()
	defer mu.Unlock()
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestConsumerRejectsUndecodableMessage(t *testing.T) {
	f := newConsumerFixture(t, func(context.Context, capturePayment) error { return nil })
	msg := broker.Message{ID: "bad-1", Type: "billing.unknown_command", Payload: []byte(`{}`)}
	if err := f.broker.Publish(context.Background(), "billing.commands", msg); err != nil {
		t.Fatal(err)
	}
	f.send(t, "saga/42#2", capturePayment{OrderID: "42"}) // must still get through

	f.waitHandled(t, 1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.rejected) != 1 {
		t.Errorf("rejected = %d, want 1 (undecodable message)", len(f.rejected))
	}
}
