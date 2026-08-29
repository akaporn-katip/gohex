// Package cqrspg is the Postgres adapter for the cqrs Deduplicator port:
// processed message IDs in a table, so command redeliveries are
// acknowledged without re-executing (ADR-0008).
package cqrspg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/cqrs"
)

// Schema is the DDL the adapter needs.
const Schema = `
CREATE TABLE IF NOT EXISTS processed_messages (
	id           text        PRIMARY KEY,
	processed_at timestamptz NOT NULL DEFAULT now()
);
`

// Migrate applies [Schema]. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("cqrspg: migrate: %w", err)
	}
	return nil
}

// Deduplicator implements cqrs.Deduplicator on Postgres.
type Deduplicator struct {
	pool *pgxpool.Pool
}

var _ cqrs.Deduplicator = (*Deduplicator)(nil)

func NewDeduplicator(pool *pgxpool.Pool) *Deduplicator { return &Deduplicator{pool: pool} }

func (d *Deduplicator) Processed(ctx context.Context, id string) (bool, error) {
	var seen bool
	err := d.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM processed_messages WHERE id = $1)`, id).Scan(&seen)
	if err != nil {
		return false, fmt.Errorf("cqrspg: processed %s: %w", id, err)
	}
	return seen, nil
}

func (d *Deduplicator) MarkProcessed(ctx context.Context, id string) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO processed_messages (id) VALUES ($1) ON CONFLICT (id) DO NOTHING`, id)
	if err != nil {
		return fmt.Errorf("cqrspg: mark %s: %w", id, err)
	}
	return nil
}
