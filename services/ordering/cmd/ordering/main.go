// Command ordering runs the ordering service: the system's REST edge,
// the Order aggregate, the order-fulfillment saga, and the order_summary
// read model. One binary hosts the whole hexagon; the composition lives
// here and nowhere else.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"

	kafkabroker "github.com/akaporn-katip/gohex/libs/broker-kafka"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	espostgres "github.com/akaporn-katip/gohex/libs/eventstore-postgres"
	"github.com/akaporn-katip/gohex/libs/o11y"
	"github.com/akaporn-katip/gohex/libs/projection"
	projectionpg "github.com/akaporn-katip/gohex/libs/projection-postgres"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/libs/saga"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/ordering/internal/adapters/httpapi"
	"github.com/akaporn-katip/gohex/services/ordering/internal/adapters/postgres"
	"github.com/akaporn-katip/gohex/services/ordering/internal/app"
)

func main() {
	if err := run(); err != nil {
		slog.Error("ordering exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := o11y.Init(ctx, o11y.Config{ServiceName: "ordering"})
	if err != nil {
		return err
	}
	defer shutdown(context.Background()) //nolint:errcheck // best-effort flush

	pool, err := connectPostgres(ctx, envOr("DATABASE_URL", "postgres://postgres:gohex@localhost:5432/ordering"))
	if err != nil {
		return err
	}
	defer pool.Close()
	for _, migrate := range []func(context.Context, *pgxpool.Pool) error{
		espostgres.Migrate, projectionpg.Migrate, postgres.Migrate,
	} {
		if err := migrate(ctx, pool); err != nil {
			return err
		}
	}

	registry := eventstore.NewRegistry()
	app.RegisterEvents(registry)
	store := espostgres.New(pool)
	checkpoints := espostgres.NewCheckpoints(pool)
	summaries := postgres.NewSummaryStore(pool)

	kafka, err := kafkabroker.New(envOr("KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		return err
	}
	defer kafka.Close()
	publisher := o11y.Publisher(kafka)
	subscriber := o11y.Subscriber(kafka)

	// Buses: the HTTP edge dispatches locally (ordering consumes no wire
	// commands).
	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()))
	queries := cqrs.NewQueryBus(o11y.QueryMiddleware())
	app.New(store, registry, summaries).Register(bus, queries)

	// Relay: publishes ordering facts and routes the saga's commands.
	rly := relay.New(store, registry, publisher, checkpoints, relay.Config{
		Name:  "ordering.relay",
		Topic: contracts.TopicOrderingEvents,
	})
	app.RegisterTranslators(rly)

	// Saga: the fulfillment workflow, driven by integration events.
	fulfillment := saga.NewRunner(app.NewFulfillmentSaga(), store, registry)

	// Projection: order_summary from own events + foreign facts via the
	// inbox (ADR-0006).
	summary := app.NewOrderSummaryProjection(summaries)
	inbox := projectionpg.NewInbox(pool)
	catchUp := projection.NewCatchUp(summary, store, registry, checkpoints, projection.Config{})
	inboxReader := projection.NewInboxReader(summary, inbox, checkpoints, projection.Config{})

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return rly.Run(ctx) })
	g.Go(func() error {
		return fulfillment.Run(ctx, subscriber, "ordering.order_fulfillment", app.SagaTopics...)
	})
	g.Go(func() error { return catchUp.Run(ctx) })
	g.Go(func() error { return inboxReader.Run(ctx) })
	for _, topic := range app.ForeignTopics {
		writer := projection.NewInboxWriter(subscriber, inbox, topic, "ordering.order_summary")
		g.Go(func() error { return writer.Run(ctx) })
	}
	g.Go(func() error { return serveHTTP(ctx, envOr("PORT", "8080"), httpapi.New(bus, queries)) })

	slog.Info("ordering running")
	return g.Wait()
}

func connectPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for range 30 {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, fmt.Errorf("postgres unreachable: %w", lastErr)
}

func serveHTTP(ctx context.Context, port string, handler http.Handler) error {
	srv := &http.Server{Addr: ":" + port, Handler: handler}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
