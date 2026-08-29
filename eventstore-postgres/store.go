// Package espostgres is the Postgres adapter for the eventstore port
// (ADR-0003): an append-only table with optimistic concurrency on
// (category, stream_id, version) and a global sequence for tailing.
package espostgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/eventstore"
)

// Schema is the DDL the adapter needs. Apply it with [Migrate] or your
// own migration tooling.
const Schema = `
CREATE TABLE IF NOT EXISTS events (
	global_seq     bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	category       text        NOT NULL,
	stream_id      text        NOT NULL,
	version        bigint      NOT NULL,
	event_name     text        NOT NULL,
	schema_version int         NOT NULL,
	payload        jsonb       NOT NULL,
	metadata       jsonb       NOT NULL DEFAULT '{}',
	occurred_at    timestamptz NOT NULL DEFAULT now(),
	UNIQUE (category, stream_id, version)
);

CREATE TABLE IF NOT EXISTS checkpoints (
	name       text        PRIMARY KEY,
	seq        bigint      NOT NULL,
	updated_at timestamptz NOT NULL DEFAULT now()
);
`

// Migrate applies [Schema]. Idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("espostgres: migrate: %w", err)
	}
	return nil
}

// appendLockKey serializes appends store-wide (advisory transaction
// lock). This guarantees the port's tailing contract — events become
// visible in GlobalSeq order — which a plain sequence cannot: without
// serialization, a transaction holding seq N could commit after N+1,
// making a tailing reader that already passed N+1 skip N forever.
// The cost is single-writer append throughput; revisit with per-stream
// locking plus a safe-watermark read if it ever becomes the bottleneck.
const appendLockKey = 0x676f686578 // "gohex"

// Store implements eventstore.Store on Postgres.
type Store struct {
	pool *pgxpool.Pool
}

var _ eventstore.Store = (*Store)(nil)

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Append(ctx context.Context, stream eventstore.StreamID, expectedVersion int64, events []eventstore.EventData) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("espostgres: append: begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // rollback after commit is a no-op

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, appendLockKey); err != nil {
		return fmt.Errorf("espostgres: append: lock: %w", err)
	}

	var current int64
	err = tx.QueryRow(ctx,
		`SELECT coalesce(max(version), 0) FROM events WHERE category = $1 AND stream_id = $2`,
		stream.Category, stream.ID).Scan(&current)
	if err != nil {
		return fmt.Errorf("espostgres: append: current version: %w", err)
	}
	if current != expectedVersion {
		return fmt.Errorf("%w: stream %s/%s at version %d, expected %d",
			eventstore.ErrVersionConflict, stream.Category, stream.ID, current, expectedVersion)
	}

	for i, e := range events {
		metadata, err := marshalMetadata(e.Metadata)
		if err != nil {
			return fmt.Errorf("espostgres: append: metadata: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO events (category, stream_id, version, event_name, schema_version, payload, metadata)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			stream.Category, stream.ID, expectedVersion+int64(i)+1,
			e.EventName, e.SchemaVersion, e.Payload, metadata)
		if err != nil {
			return fmt.Errorf("espostgres: append: insert: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("espostgres: append: commit: %w", err)
	}
	return nil
}

func (s *Store) Load(ctx context.Context, stream eventstore.StreamID, afterVersion int64) ([]eventstore.RecordedEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT global_seq, category, stream_id, version, event_name, schema_version, payload, metadata, occurred_at
		FROM events
		WHERE category = $1 AND stream_id = $2 AND version > $3
		ORDER BY version`,
		stream.Category, stream.ID, afterVersion)
	if err != nil {
		return nil, fmt.Errorf("espostgres: load: %w", err)
	}
	return scanEvents(rows)
}

func (s *Store) ReadAll(ctx context.Context, afterSeq int64, limit int) ([]eventstore.RecordedEvent, error) {
	q := `
		SELECT global_seq, category, stream_id, version, event_name, schema_version, payload, metadata, occurred_at
		FROM events
		WHERE global_seq > $1
		ORDER BY global_seq`
	args := []any{afterSeq}
	if limit > 0 {
		q += ` LIMIT $2`
		args = append(args, limit)
	}
	rows, err := s.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("espostgres: readall: %w", err)
	}
	return scanEvents(rows)
}

func scanEvents(rows pgx.Rows) ([]eventstore.RecordedEvent, error) {
	defer rows.Close()
	var out []eventstore.RecordedEvent
	for rows.Next() {
		var rec eventstore.RecordedEvent
		var metadata []byte
		if err := rows.Scan(&rec.GlobalSeq, &rec.Stream.Category, &rec.Stream.ID, &rec.Version,
			&rec.EventName, &rec.SchemaVersion, &rec.Payload, &metadata, &rec.OccurredAt); err != nil {
			return nil, fmt.Errorf("espostgres: scan: %w", err)
		}
		if err := unmarshalMetadata(metadata, &rec.Metadata); err != nil {
			return nil, fmt.Errorf("espostgres: scan metadata: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("espostgres: rows: %w", err)
	}
	return out, nil
}

func marshalMetadata(m map[string]string) ([]byte, error) {
	if len(m) == 0 {
		return []byte(`{}`), nil
	}
	return json.Marshal(m)
}

func unmarshalMetadata(data []byte, into *map[string]string) error {
	if len(data) == 0 || string(data) == `{}` {
		return nil
	}
	if err := json.Unmarshal(data, into); err != nil {
		return err
	}
	if len(*into) == 0 {
		*into = nil
	}
	return nil
}
