package cqrspg_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	cqrspg "github.com/akaporn-katip/gohex/cqrs-postgres"
)

func TestDeduplicator(t *testing.T) {
	dsn := os.Getenv("GOHEX_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GOHEX_POSTGRES_TEST_DSN to run Postgres tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := cqrspg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE processed_messages`); err != nil {
		t.Fatal(err)
	}

	d := cqrspg.NewDeduplicator(pool)
	if seen, _ := d.Processed(ctx, "m1"); seen {
		t.Fatal("fresh id reported processed")
	}
	if err := d.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := d.MarkProcessed(ctx, "m1"); err != nil {
		t.Fatal("re-mark must be a no-op, got:", err)
	}
	if seen, _ := d.Processed(ctx, "m1"); !seen {
		t.Fatal("marked id not reported processed")
	}
	if seen, _ := d.Processed(ctx, "m2"); seen {
		t.Fatal("other id reported processed")
	}
}
