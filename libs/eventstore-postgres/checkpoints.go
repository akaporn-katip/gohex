package espostgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/libs/eventstore"
)

// Checkpoints implements eventstore.CheckpointStore on Postgres.
type Checkpoints struct {
	pool *pgxpool.Pool
}

var _ eventstore.CheckpointStore = (*Checkpoints)(nil)

func NewCheckpoints(pool *pgxpool.Pool) *Checkpoints { return &Checkpoints{pool: pool} }

func (c *Checkpoints) Get(ctx context.Context, name string) (int64, error) {
	var seq int64
	err := c.pool.QueryRow(ctx,
		`SELECT coalesce((SELECT seq FROM checkpoints WHERE name = $1), 0)`, name).Scan(&seq)
	if err != nil {
		return 0, fmt.Errorf("espostgres: checkpoint get %s: %w", name, err)
	}
	return seq, nil
}

func (c *Checkpoints) Set(ctx context.Context, name string, seq int64) error {
	_, err := c.pool.Exec(ctx, `
		INSERT INTO checkpoints (name, seq) VALUES ($1, $2)
		ON CONFLICT (name) DO UPDATE SET seq = excluded.seq, updated_at = now()`,
		name, seq)
	if err != nil {
		return fmt.Errorf("espostgres: checkpoint set %s: %w", name, err)
	}
	return nil
}
