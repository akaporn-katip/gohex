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

	"github.com/akaporn-katip/gohex-example/contracts"
	"github.com/akaporn-katip/gohex-example/ordering/internal/adapters/httpapi"
	"github.com/akaporn-katip/gohex-example/ordering/internal/adapters/postgres"
	"github.com/akaporn-katip/gohex-example/ordering/internal/app"
	"github.com/akaporn-katip/gohex/broker"
	kafkabroker "github.com/akaporn-katip/gohex/broker-kafka"
	"github.com/akaporn-katip/gohex/cqrs"
	"github.com/akaporn-katip/gohex/eventstore"
	espostgres "github.com/akaporn-katip/gohex/eventstore-postgres"
	"github.com/akaporn-katip/gohex/featureflag"
	"github.com/akaporn-katip/gohex/o11y"
	"github.com/akaporn-katip/gohex/projection"
	projectionpg "github.com/akaporn-katip/gohex/projection-postgres"
	"github.com/akaporn-katip/gohex/relay"
	"github.com/akaporn-katip/gohex/saga"
	"github.com/akaporn-katip/gohex/supervise"
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
		espostgres.Migrate, projectionpg.Migrate, postgres.Migrate, postgres.MigrateNotifications,
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
	notifications := postgres.NewNotificationQueue(pool)

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

	// Feature flags: Install stamps whatever the edge recorded onto every
	// appended event, so the saga sees the same decision hours later
	// (gohex ADR-0021). A real deployment swaps Static for the OpenFeature
	// adapter; the edge code does not change.
	featureflag.Install()
	flags := featureflag.NewClient(featureflag.Static{
		app.FlagExpressShipping: envOr("EXPRESS_SHIPPING", "false") == "true",
	})

	// Saga: the fulfillment workflow, driven by integration events.
	fulfillment := saga.NewRunner(app.NewFulfillmentSaga(), store, registry)

	// Projection: order_summary from own events + foreign facts via the
	// inbox (ADR-0006).
	// Observe traces every apply; the runners are pollers, so each apply
	// is its own trace linked to the event that caused it (ADR-0015).
	summary := app.NewOrderSummaryProjection(summaries, notifications)
	inbox := projectionpg.NewInbox(pool)
	projectionCfg := projection.Config{Observe: o11y.ProjectionHook()}
	catchUp := projection.NewCatchUp(summary, store, registry, checkpoints, projectionCfg)
	inboxReader := projection.NewInboxReader(summary, inbox, checkpoints, projectionCfg)

	// Notifier: the polling worker draining the notification worklist the
	// projection fills. Its work links back to the originating request
	// rather than continuing its trace (ADR-0015).
	notifier := app.NewNotifier(notifications, bus, app.NotifierConfig{})

	// Supervisor: every background runner gets its own restart loop
	// (ADR-0019). These runners all keep a durable checkpoint and promise
	// to resume from it, so a failure is a restart, not an outage — and
	// NOT an errgroup, which cancels every sibling (the HTTP server
	// included) over one message a projection cannot apply.
	sup := supervise.New(supervise.Config{
		Restarted: o11y.RunnerRestartHook(),
		Report:    reportRunnerFailure,
	})
	sup.Go(ctx, "ordering.relay", rly.Run)
	sup.Go(ctx, "ordering.order_fulfillment", func(ctx context.Context) error {
		return fulfillment.Run(ctx, subscriber, "ordering.order_fulfillment", app.SagaTopics...)
	})
	sup.Go(ctx, "ordering.order_summary.catchup", catchUp.Run)
	sup.Go(ctx, "ordering.order_summary.inbox", inboxReader.Run)
	sup.Go(ctx, "ordering.notifier", notifier.Run)
	for _, topic := range app.ForeignTopics {
		writer := projection.NewInboxWriter(subscriber, inbox, topic, "ordering.order_summary",
			projection.InboxWriterConfig{OnRejected: rejectedForeignFact(topic)})
		sup.Go(ctx, "ordering.order_summary.inbox_writer."+topic, writer.Run)
	}

	slog.Info("ordering running")

	// The HTTP server is the one runner whose failure IS the process's: a
	// service that cannot serve its API has nothing left to supervise.
	// Deciding that is the composition root's job and nobody else's.
	err = serveHTTP(ctx, envOr("PORT", "8080"), httpapi.New(bus, queries, flags))
	stop()     // whatever ended the server, the runners wind down with it
	sup.Wait() // Wait only waits; the context above is what cancels
	return err
}

// reportRunnerFailure is the policy half of the supervisor: gohex counts
// every restart (gohex.runner.restarts) and decides which failures are
// news — the first one, and the first one that reads differently — and
// the service decides what a news-worthy failure costs. Logging at Error
// with the retry delay is the floor; a production service alerts on the
// restart counter, because a log nobody reads is how a wedged projection
// stays invisible. A recovered panic arrives here too, with its stack.
func reportRunnerFailure(ctx context.Context, f supervise.Failure) {
	fields := []any{"runner", f.Runner, "retry_in", f.RetryIn.String(), "error", f.Err}
	if f.Stack != nil {
		fields = append(fields, "stack", string(f.Stack))
	}
	slog.ErrorContext(ctx, "background runner failed and will be restarted", fields...)
}

// rejectedForeignFact reports an envelope the inbox will never accept —
// no ID, no type, version 0, or a payload that is not JSON (ADR-0018).
// The writer acknowledges it and moves on, because redelivering it
// forever would freeze every later fact on that partition, so THIS is
// the only place the message is ever mentioned: a silent callback means
// a foreign fact vanished with nobody watching.
//
// gohex supplies the mechanism and the service owns the policy. Logging
// loudly is the floor; a production service writes the envelope to a
// rejected-messages table and alerts on the count, so a misbehaving
// producer is found before anyone notices a stale read model.
func rejectedForeignFact(topic string) func(context.Context, broker.Message, error) {
	return func(ctx context.Context, msg broker.Message, err error) {
		slog.ErrorContext(ctx, "rejected foreign fact",
			"topic", topic,
			"group", "ordering.order_summary",
			"message_id", msg.ID,
			"type", msg.Type,
			"version", msg.Version,
			"error", err)
	}
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
