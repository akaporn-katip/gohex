// Package ports holds the driven interfaces the application layer owns;
// adapters implement them (ADR: hexagon layout).
package ports

import (
	"context"
	"time"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// ErrOrderNotFound maps to 404 at the edge via its code.
var ErrOrderNotFound = kernel.NewDomainError("not_found", "order does not exist")

// Status ranks: a projection handler may only raise the rank, which
// keeps status updates commutative across the two event sources
// (ADR-0006). Failure statuses outrank progress ones.
const (
	RankPlaced        = 1
	RankPaid          = 2
	RankReserved      = 3
	RankShipped       = 4
	RankPaymentFailed = 10
	RankRejected      = 11
	RankRefunded      = 12
)

// OrderSummary is one row of the order_summary read model.
type OrderSummary struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	Cents      int64     `json:"cents"`
	Currency   string    `json:"currency"`
	Qty        int       `json:"qty"`
	Status     string    `json:"status"`
	PlacedAt   time.Time `json:"placed_at"`
}

// SummaryStore is the read-model port. Both writers are commutative:
// UpsertPlaced never downgrades a status a foreign event already set,
// and SetStatus only raises the rank.
type SummaryStore interface {
	// UpsertPlaced writes the base columns and sets status "placed"
	// unless a higher-ranked status is already present.
	UpsertPlaced(ctx context.Context, s OrderSummary) error
	// SetStatus raises the order's status to (status, rank); lower or
	// equal ranks are ignored. The row is created if missing.
	SetStatus(ctx context.Context, orderID, status string, rank int) error
	// Get returns the summary or ErrOrderNotFound.
	Get(ctx context.Context, orderID string) (OrderSummary, error)
}
