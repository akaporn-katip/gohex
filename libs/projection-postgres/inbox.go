// Package projectionpg is the Postgres adapter for the projection Inbox
// port: consumed foreign integration events stored durably in arrival
// order, so projections rebuild from the inbox rather than the broker
// (ADR-0006).
package projectionpg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/projection"
)

// Schema is the DDL the adapter needs.
const Schema = `
CREATE TABLE IF NOT EXISTS inbox (
	seq         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	id          text        NOT NULL UNIQUE,
	message     jsonb       NOT NULL,
	received_at timestamptz NOT NULL DEFAULT now()
);
`

// Migrate applies [Schema]. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("projectionpg: migrate: %w", err)
	}
	return nil
}

// Inbox implements projection.Inbox on Postgres.
type Inbox struct {
	pool *pgxpool.Pool
}

var _ projection.Inbox = (*Inbox)(nil)

func NewInbox(pool *pgxpool.Pool) *Inbox { return &Inbox{pool: pool} }

func (i *Inbox) Append(ctx context.Context, msg broker.Message) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("projectionpg: encoding %s: %w", msg.ID, err)
	}
	_, err = i.pool.Exec(ctx,
		`INSERT INTO inbox (id, message) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`,
		msg.ID, payload)
	if err != nil {
		return fmt.Errorf("projectionpg: append %s: %w", msg.ID, err)
	}
	return nil
}

func (i *Inbox) ReadAll(ctx context.Context, afterSeq int64, limit int) ([]projection.InboxMessage, error) {
	q := `SELECT seq, message FROM inbox WHERE seq > $1 ORDER BY seq`
	args := []any{afterSeq}
	if limit > 0 {
		q += ` LIMIT $2`
		args = append(args, limit)
	}
	rows, err := i.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("projectionpg: readall: %w", err)
	}
	return scanInbox(rows)
}

func scanInbox(rows pgx.Rows) ([]projection.InboxMessage, error) {
	defer rows.Close()
	var out []projection.InboxMessage
	for rows.Next() {
		var m projection.InboxMessage
		var payload []byte
		if err := rows.Scan(&m.Seq, &payload); err != nil {
			return nil, fmt.Errorf("projectionpg: scan: %w", err)
		}
		if err := json.Unmarshal(payload, &m.Message); err != nil {
			return nil, fmt.Errorf("projectionpg: decode seq %d: %w", m.Seq, err)
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("projectionpg: rows: %w", err)
	}
	return out, nil
}
