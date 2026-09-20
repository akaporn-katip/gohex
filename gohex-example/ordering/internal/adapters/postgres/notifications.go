package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex-example/ordering/internal/ports"
)

// NotificationSchema is the worklist's DDL. The metadata column is the
// interesting part: it holds the ORIGIN trace context of the fact that
// created the row, so the worker that picks it up seconds later can link
// its work back to the request that caused it (ADR-0015). It carries no
// business data — same rule as envelope metadata (ADR-0005).
const NotificationSchema = `
CREATE TABLE IF NOT EXISTS pending_notification (
	id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	order_id   text        NOT NULL,
	reason     text        NOT NULL,
	metadata   jsonb       NOT NULL DEFAULT '{}'::jsonb,
	created_at timestamptz NOT NULL DEFAULT now(),
	sent_at    timestamptz,
	UNIQUE (order_id, reason)
);
CREATE INDEX IF NOT EXISTS pending_notification_unsent
	ON pending_notification (id) WHERE sent_at IS NULL;
`

// MigrateNotifications applies [NotificationSchema]. Idempotent.
func MigrateNotifications(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, NotificationSchema); err != nil {
		return fmt.Errorf("ordering/postgres: migrate notifications: %w", err)
	}
	return nil
}

// NotificationQueue implements ports.NotificationQueue.
type NotificationQueue struct {
	pool *pgxpool.Pool
}

var _ ports.NotificationQueue = (*NotificationQueue)(nil)

func NewNotificationQueue(pool *pgxpool.Pool) *NotificationQueue {
	return &NotificationQueue{pool: pool}
}

// Enqueue adds the milestone to the worklist. A redelivered projection
// apply collapses onto the existing row (and keeps its original
// metadata: the first sighting is the causal one).
func (q *NotificationQueue) Enqueue(ctx context.Context, orderID, reason string, metadata map[string]string) error {
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("ordering/postgres: encoding metadata for %s: %w", orderID, err)
	}
	_, err = q.pool.Exec(ctx, `
		INSERT INTO pending_notification (order_id, reason, metadata)
		VALUES ($1, $2, $3)
		ON CONFLICT (order_id, reason) DO NOTHING`,
		orderID, reason, encoded)
	if err != nil {
		return fmt.Errorf("ordering/postgres: enqueue %s/%s: %w", orderID, reason, err)
	}
	return nil
}

func (q *NotificationQueue) Pending(ctx context.Context, limit int) ([]ports.PendingNotification, error) {
	rows, err := q.pool.Query(ctx, `
		SELECT id, order_id, reason, metadata
		FROM pending_notification
		WHERE sent_at IS NULL
		ORDER BY id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("ordering/postgres: pending notifications: %w", err)
	}
	return scanPending(rows)
}

func scanPending(rows pgx.Rows) ([]ports.PendingNotification, error) {
	defer rows.Close()
	var out []ports.PendingNotification
	for rows.Next() {
		var p ports.PendingNotification
		var metadata []byte
		if err := rows.Scan(&p.ID, &p.OrderID, &p.Reason, &metadata); err != nil {
			return nil, fmt.Errorf("ordering/postgres: scan pending: %w", err)
		}
		if err := json.Unmarshal(metadata, &p.Metadata); err != nil {
			return nil, fmt.Errorf("ordering/postgres: decode metadata for %d: %w", p.ID, err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ordering/postgres: pending rows: %w", err)
	}
	return out, nil
}

func (q *NotificationQueue) MarkSent(ctx context.Context, id int64) error {
	_, err := q.pool.Exec(ctx,
		`UPDATE pending_notification SET sent_at = now() WHERE id = $1 AND sent_at IS NULL`, id)
	if err != nil {
		return fmt.Errorf("ordering/postgres: mark sent %d: %w", id, err)
	}
	return nil
}
