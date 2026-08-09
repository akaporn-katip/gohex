package cqrs_test

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/libs/cqrs"
)

type orderSummaryQuery struct {
	OrderID string
}

func (orderSummaryQuery) QueryName() string { return "ordering.order_summary" }

type orderSummary struct {
	OrderID string
	Status  string
}

func TestAskReturnsTypedResult(t *testing.T) {
	bus := cqrs.NewQueryBus()
	cqrs.HandleQuery(bus, func(_ context.Context, q orderSummaryQuery) (orderSummary, error) {
		return orderSummary{OrderID: q.OrderID, Status: "paid"}, nil
	})

	got, err := cqrs.Ask[orderSummary](context.Background(), bus, orderSummaryQuery{OrderID: "42"})
	if err != nil {
		t.Fatalf("Ask: %v", err)
	}
	if got.OrderID != "42" || got.Status != "paid" {
		t.Errorf("Ask = %+v", got)
	}
}

func TestAskWrongTypeFails(t *testing.T) {
	bus := cqrs.NewQueryBus()
	cqrs.HandleQuery(bus, func(_ context.Context, q orderSummaryQuery) (orderSummary, error) {
		return orderSummary{}, nil
	})
	if _, err := cqrs.Ask[string](context.Background(), bus, orderSummaryQuery{}); err == nil {
		t.Fatal("Ask with wrong type parameter = nil error")
	}
}

func TestQueryUnhandled(t *testing.T) {
	bus := cqrs.NewQueryBus()
	if _, err := bus.Dispatch(context.Background(), orderSummaryQuery{}); err == nil {
		t.Fatal("Dispatch(unhandled) = nil error")
	}
}

func TestQueryMiddleware(t *testing.T) {
	var seen []string
	logging := func(next cqrs.QueryHandlerFunc) cqrs.QueryHandlerFunc {
		return func(ctx context.Context, q cqrs.Query) (any, error) {
			seen = append(seen, q.QueryName())
			return next(ctx, q)
		}
	}
	bus := cqrs.NewQueryBus(logging)
	cqrs.HandleQuery(bus, func(_ context.Context, q orderSummaryQuery) (orderSummary, error) {
		return orderSummary{}, nil
	})

	if _, err := cqrs.Ask[orderSummary](context.Background(), bus, orderSummaryQuery{}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] != "ordering.order_summary" {
		t.Errorf("middleware saw %v", seen)
	}
}
