package espostgres_test

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/libs/eventstore"
	espostgres "github.com/akaporn-katip/gohex/libs/eventstore-postgres"
	"github.com/akaporn-katip/gohex/libs/eventstore/storetest"
)

// TestPostgresStoreContract runs the shared Store contract suite against
// a real Postgres. Point GOHEX_POSTGRES_TEST_DSN at a disposable
// database, e.g.:
//
//	docker run -d --rm -e POSTGRES_PASSWORD=gohex -p 55432:5432 postgres:16-alpine
//	GOHEX_POSTGRES_TEST_DSN='postgres://postgres:gohex@localhost:55432/postgres' go test ./...
func TestPostgresStoreContract(t *testing.T) {
	dsn := os.Getenv("GOHEX_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set GOHEX_POSTGRES_TEST_DSN to run Postgres contract tests")
	}
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := espostgres.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	storetest.Run(t, func(t *testing.T) eventstore.Store {
		if _, err := pool.Exec(ctx, `TRUNCATE events RESTART IDENTITY`); err != nil {
			t.Fatalf("truncate: %v", err)
		}
		return espostgres.New(pool)
	})
}
