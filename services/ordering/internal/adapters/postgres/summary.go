// Package postgres holds ordering's driven adapters: the order_summary
// read-model store implementing ports.SummaryStore.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/services/ordering/internal/ports"
)

// Schema is the read model's DDL. Rebuildable at will: truncate +
// projection.Reset + restart (ADR-0006).
const Schema = `
CREATE TABLE IF NOT EXISTS order_summary (
	order_id    text PRIMARY KEY,
	customer_id text NOT NULL DEFAULT '',
	cents       bigint NOT NULL DEFAULT 0,
	currency    text NOT NULL DEFAULT '',
	qty         int NOT NULL DEFAULT 0,
	status      text NOT NULL DEFAULT '',
	status_rank int NOT NULL DEFAULT 0,
	placed_at   timestamptz,
	updated_at  timestamptz NOT NULL DEFAULT now()
);
`

// Migrate applies [Schema]. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("ordering/postgres: migrate: %w", err)
	}
	return nil
}

// SummaryStore implements ports.SummaryStore. Both writers are
// column-wise, rank-guarded upserts — idempotent AND commutative, as the
// projection contract requires.
type SummaryStore struct {
	pool *pgxpool.Pool
}

var _ ports.SummaryStore = (*SummaryStore)(nil)

func NewSummaryStore(pool *pgxpool.Pool) *SummaryStore { return &SummaryStore{pool: pool} }

func (s *SummaryStore) UpsertPlaced(ctx context.Context, o ports.OrderSummary) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO order_summary (order_id, customer_id, cents, currency, qty, status, status_rank, placed_at)
		VALUES ($1, $2, $3, $4, $5, 'placed', $6, $7)
		ON CONFLICT (order_id) DO UPDATE SET
			customer_id = EXCLUDED.customer_id,
			cents       = EXCLUDED.cents,
			currency    = EXCLUDED.currency,
			qty         = EXCLUDED.qty,
			placed_at   = EXCLUDED.placed_at,
			-- never downgrade a status a foreign event already set
			status      = CASE WHEN order_summary.status_rank >= EXCLUDED.status_rank
			                   THEN order_summary.status ELSE EXCLUDED.status END,
			status_rank = GREATEST(order_summary.status_rank, EXCLUDED.status_rank),
			updated_at  = now()`,
		o.OrderID, o.CustomerID, o.Cents, o.Currency, o.Qty, ports.RankPlaced, o.PlacedAt)
	if err != nil {
		return fmt.Errorf("ordering/postgres: upsert %s: %w", o.OrderID, err)
	}
	return nil
}

func (s *SummaryStore) SetStatus(ctx context.Context, orderID, status string, rank int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO order_summary (order_id, status, status_rank)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO UPDATE SET
			status      = CASE WHEN order_summary.status_rank >= EXCLUDED.status_rank
			                   THEN order_summary.status ELSE EXCLUDED.status END,
			status_rank = GREATEST(order_summary.status_rank, EXCLUDED.status_rank),
			updated_at  = now()`,
		orderID, status, rank)
	if err != nil {
		return fmt.Errorf("ordering/postgres: set status %s: %w", orderID, err)
	}
	return nil
}

func (s *SummaryStore) Get(ctx context.Context, orderID string) (ports.OrderSummary, error) {
	var o ports.OrderSummary
	err := s.pool.QueryRow(ctx, `
		SELECT order_id, customer_id, cents, currency, qty, status, coalesce(placed_at, 'epoch'::timestamptz)
		FROM order_summary WHERE order_id = $1`, orderID).
		Scan(&o.OrderID, &o.CustomerID, &o.Cents, &o.Currency, &o.Qty, &o.Status, &o.PlacedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ports.OrderSummary{}, fmt.Errorf("%w: %s", ports.ErrOrderNotFound, orderID)
	}
	if err != nil {
		return ports.OrderSummary{}, fmt.Errorf("ordering/postgres: get %s: %w", orderID, err)
	}
	return o, nil
}
