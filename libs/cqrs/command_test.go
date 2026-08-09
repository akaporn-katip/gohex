package cqrs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/kernel"
)

type capturePayment struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (capturePayment) CommandName() string { return "billing.capture_payment" }

var errDeclined = kernel.NewDomainError("payment_declined", "card declined")

func fastRetry(attempts int) cqrs.RetryPolicy {
	return cqrs.RetryPolicy{Attempts: attempts, Backoff: time.Millisecond}
}

func TestDispatchRoutesToHandler(t *testing.T) {
	bus := cqrs.NewBus()
	var got capturePayment
	cqrs.Handle(bus, func(_ context.Context, cmd capturePayment) error {
		got = cmd
		return nil
	})

	if err := bus.Dispatch(context.Background(), capturePayment{OrderID: "42", Cents: 100}); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if got.OrderID != "42" || got.Cents != 100 {
		t.Errorf("handler got %+v", got)
	}
}

func TestDispatchUnhandledCommand(t *testing.T) {
	bus := cqrs.NewBus()
	if err := bus.Dispatch(context.Background(), capturePayment{}); err == nil {
		t.Fatal("Dispatch(unhandled) = nil error")
	}
}

func TestHandleDuplicatePanics(t *testing.T) {
	bus := cqrs.NewBus()
	cqrs.Handle(bus, func(context.Context, capturePayment) error { return nil })
	defer func() {
		if recover() == nil {
			t.Error("duplicate Handle did not panic")
		}
	}()
	cqrs.Handle(bus, func(context.Context, capturePayment) error { return nil })
}

func TestTransientErrorsAreRetried(t *testing.T) {
	bus := cqrs.NewBus(cqrs.WithRetry(fastRetry(3)))
	calls := 0
	cqrs.Handle(bus, func(context.Context, capturePayment) error {
		calls++
		if calls < 3 {
			return errors.New("version conflict")
		}
		return nil
	})

	if err := bus.Dispatch(context.Background(), capturePayment{}); err != nil {
		t.Fatalf("Dispatch = %v, want nil after retries", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestRetriesExhaust(t *testing.T) {
	bus := cqrs.NewBus(cqrs.WithRetry(fastRetry(3)))
	calls := 0
	transient := errors.New("still down")
	cqrs.Handle(bus, func(context.Context, capturePayment) error {
		calls++
		return transient
	})

	if err := bus.Dispatch(context.Background(), capturePayment{}); !errors.Is(err, transient) {
		t.Fatalf("Dispatch = %v, want the transient error", err)
	}
	if calls != 3 {
		t.Errorf("calls = %d, want 3", calls)
	}
}

func TestDomainErrorsAreNeverRetried(t *testing.T) {
	bus := cqrs.NewBus(cqrs.WithRetry(fastRetry(5)))
	calls := 0
	cqrs.Handle(bus, func(context.Context, capturePayment) error {
		calls++
		return errDeclined
	})

	err := bus.Dispatch(context.Background(), capturePayment{})
	if !errors.Is(err, errDeclined) {
		t.Fatalf("Dispatch = %v, want errDeclined", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (definitive rejections must not retry)", calls)
	}
}

func TestCustomShouldRetry(t *testing.T) {
	special := errors.New("special")
	bus := cqrs.NewBus(cqrs.WithRetry(cqrs.RetryPolicy{
		Attempts: 4, Backoff: time.Millisecond,
		ShouldRetry: func(err error) bool { return errors.Is(err, special) },
	}))
	calls := 0
	cqrs.Handle(bus, func(context.Context, capturePayment) error {
		calls++
		if calls == 1 {
			return special
		}
		return errors.New("other") // not retryable under the custom policy
	})

	if err := bus.Dispatch(context.Background(), capturePayment{}); err == nil {
		t.Fatal("Dispatch = nil, want error")
	}
	if calls != 2 {
		t.Errorf("calls = %d, want 2", calls)
	}
}

func TestMiddlewareOrderAndSingleObservation(t *testing.T) {
	var order []string
	mw := func(name string) cqrs.Middleware {
		return func(next cqrs.HandlerFunc) cqrs.HandlerFunc {
			return func(ctx context.Context, cmd cqrs.Command) error {
				order = append(order, name)
				return next(ctx, cmd)
			}
		}
	}
	bus := cqrs.NewBus(cqrs.WithMiddleware(mw("outer"), mw("inner")), cqrs.WithRetry(fastRetry(3)))
	calls := 0
	cqrs.Handle(bus, func(context.Context, capturePayment) error {
		calls++
		order = append(order, "handler")
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})

	if err := bus.Dispatch(context.Background(), capturePayment{}); err != nil {
		t.Fatal(err)
	}
	// Middleware observes ONE dispatch; the retry happens inside it.
	want := []string{"outer", "inner", "handler", "handler"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}
