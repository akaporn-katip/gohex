// Command inventory runs the inventory service: it consumes commands
// from inventory.commands, decides stock reservations, and publishes
// inventory facts to inventory.events via the relay. Its only
// synchronous surface is /healthz — services talk to inventory through
// the broker (ADR-0008).
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

	"github.com/akaporn-katip/gohex/libs/broker"
	kafkabroker "github.com/akaporn-katip/gohex/libs/broker-kafka"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	cqrspg "github.com/akaporn-katip/gohex/libs/cqrs-postgres"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	espostgres "github.com/akaporn-katip/gohex/libs/eventstore-postgres"
	"github.com/akaporn-katip/gohex/libs/o11y"
	"github.com/akaporn-katip/gohex/libs/relay"
	"github.com/akaporn-katip/gohex/services/contracts"
	"github.com/akaporn-katip/gohex/services/inventory/internal/app"
)

func main() {
	if err := run(); err != nil {
		slog.Error("inventory exited", "error", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdown, err := o11y.Init(ctx, o11y.Config{ServiceName: "inventory"})
	if err != nil {
		return err
	}
	defer shutdown(context.Background()) //nolint:errcheck // best-effort flush

	pool, err := connectPostgres(ctx, envOr("DATABASE_URL", "postgres://postgres:gohex@localhost:5432/inventory"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := espostgres.Migrate(ctx, pool); err != nil {
		return err
	}
	if err := cqrspg.Migrate(ctx, pool); err != nil {
		return err
	}

	registry := eventstore.NewRegistry()
	app.RegisterEvents(registry)
	store := espostgres.New(pool)

	kafka, err := kafkabroker.New(envOr("KAFKA_BROKERS", "localhost:9092"))
	if err != nil {
		return err
	}
	defer kafka.Close()
	publisher := o11y.Publisher(kafka)
	subscriber := o11y.Subscriber(kafka)

	// Command side: bus + wire consumer with dedup.
	bus := cqrs.NewBus(cqrs.WithMiddleware(o11y.CommandMiddleware()))
	app.New(store, registry).Register(bus)
	commandRegistry := cqrs.NewRegistry()
	app.RegisterCommands(commandRegistry)
	consumer := cqrs.NewConsumer(bus, commandRegistry, cqrspg.NewDeduplicator(pool), cqrs.ConsumerConfig{
		OnRejected: func(ctx context.Context, msg broker.Message, err error) {
			slog.WarnContext(ctx, "command rejected", "message_id", msg.ID, "type", msg.Type, "error", err)
		},
	})

	// Event side: the relay publishes inventory facts.
	rly := relay.New(store, registry, publisher, espostgres.NewCheckpoints(pool), relay.Config{
		Name:  "inventory.relay",
		Topic: contracts.TopicInventoryEvents,
	})
	app.RegisterTranslators(rly)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return consumer.Run(ctx, subscriber, contracts.TopicInventoryCommands, "inventory") })
	g.Go(func() error { return rly.Run(ctx) })
	g.Go(func() error { return serveHealth(ctx, envOr("PORT", "8082")) })

	slog.Info("inventory running")
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

func serveHealth(ctx context.Context, port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}
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
