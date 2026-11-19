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

	"github.com/akaporn-katip/gohex-example/contracts"
	"github.com/akaporn-katip/gohex-example/inventory/internal/app"
	"github.com/akaporn-katip/gohex/broker"
	kafkabroker "github.com/akaporn-katip/gohex/broker-kafka"
	"github.com/akaporn-katip/gohex/cqrs"
	cqrspg "github.com/akaporn-katip/gohex/cqrs-postgres"
	"github.com/akaporn-katip/gohex/eventstore"
	espostgres "github.com/akaporn-katip/gohex/eventstore-postgres"
	"github.com/akaporn-katip/gohex/o11y"
	"github.com/akaporn-katip/gohex/relay"
	"github.com/akaporn-katip/gohex/supervise"
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

	// Supervisor: the consumer and the relay each get their own restart
	// loop (ADR-0019). Both keep a durable checkpoint and resume from it,
	// so one failure is a restart of that runner — not an errgroup, which
	// would cancel the siblings and the health endpoint with them, turning
	// one unprocessable command into CrashLoopBackOff.
	sup := supervise.New(supervise.Config{
		Restarted: o11y.RunnerRestartHook(),
		Report:    reportRunnerFailure,
	})
	sup.Go(ctx, "inventory.commands", func(ctx context.Context) error {
		return consumer.Run(ctx, subscriber, contracts.TopicInventoryCommands, "inventory")
	})
	sup.Go(ctx, "inventory.relay", rly.Run)

	slog.Info("inventory running")

	// The health endpoint is the one runner whose failure IS the
	// process's: a service Kubernetes cannot probe is a service it cannot
	// route to. What is fatal stays the composition root's judgement.
	err = serveHealth(ctx, envOr("PORT", "8082"))
	stop()     // whatever ended the server, the runners wind down with it
	sup.Wait() // Wait only waits; the context above is what cancels
	return err
}

// reportRunnerFailure is the policy half of the supervisor: gohex counts
// every restart (gohex.runner.restarts) and decides which failures are
// news — the first one, and the first one that reads differently — and
// the service decides what a news-worthy failure costs. Logging at Error
// with the retry delay is the floor; a production service alerts on the
// restart counter, because a log nobody reads is how a wedged runner
// stays invisible. A recovered panic arrives here too, with its stack.
func reportRunnerFailure(ctx context.Context, f supervise.Failure) {
	fields := []any{"runner", f.Runner, "retry_in", f.RetryIn.String(), "error", f.Err}
	if f.Stack != nil {
		fields = append(fields, "stack", string(f.Stack))
	}
	slog.ErrorContext(ctx, "background runner failed and will be restarted", fields...)
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
