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

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/projection"
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

// Append stores msg if its ID is new. The message is stored as jsonb, so
// it round-trips semantically, not byte-for-byte: Postgres normalizes
// JSON formatting (whitespace, key order) in the nested payload — the
// same tolerance the eventstore adapter has. Consumers decode with
// json.Unmarshal, which is unaffected.
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

// Head reads the inbox's high-water mark. max(seq) is a backwards scan
// of the primary key, cheap enough for a metrics scrape; coalesce turns
// an empty inbox into 0, the port's answer for holding nothing.
func (i *Inbox) Head(ctx context.Context) (int64, error) {
	var head int64
	if err := i.pool.QueryRow(ctx, `SELECT coalesce(max(seq), 0) FROM inbox`).Scan(&head); err != nil {
		return 0, fmt.Errorf("projectionpg: head: %w", err)
	}
	return head, nil
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
