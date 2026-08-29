package projection_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/projection"
)

// Own domain event.
type orderPlaced struct {
	OrderID string `json:"order_id"`
	Cents   int64  `json:"cents"`
}

func (orderPlaced) EventName() string { return "ordering.order_placed" }

// Own domain event with no handler registered.
type orderAudited struct{}

func (orderAudited) EventName() string { return "ordering.order_audited" }

// Foreign integration event from billing.
type paymentCapturedV1 struct {
	OrderID string `json:"order_id"`
}

func (paymentCapturedV1) EventName() string    { return "billing.payment_captured" }
func (paymentCapturedV1) ContractVersion() int { return 1 }

// The read model: an in-memory stand-in for an order_summary table.
type summaryRow struct {
	Cents  int64
	Status string
	Placed time.Time
}

type summaryTable struct {
	mu   sync.Mutex
	rows map[string]summaryRow
	// applied counts handler invocations to observe redelivery.
	applied int
}

func newSummaryTable() *summaryTable { return &summaryTable{rows: map[string]summaryRow{}} }

func (tb *summaryTable) get(id string) (summaryRow, bool) {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	r, ok := tb.rows[id]
	return r, ok
}

// newProjection wires the order_summary projection over the table.
func newProjection(tb *summaryTable) *projection.Projection {
	p := projection.New("order_summary")
	projection.On(p, func(_ context.Context, e orderPlaced, m projection.Meta) error {
		tb.mu.Lock()
		defer tb.mu.Unlock()
		tb.applied++
		// Column-wise upsert, like INSERT ... ON CONFLICT DO UPDATE SET
		// cents = ..., placed = ... — NOT a whole-row overwrite. The two
		// logs feeding a projection have no cross-ordering, so handlers
		// must be commutative: this one must not clobber the status a
		// foreign handler may already have written.
		row := tb.rows[e.OrderID]
		row.Cents = e.Cents
		row.Placed = m.OccurredAt
		if row.Status == "" {
			row.Status = "placed"
		}
		tb.rows[e.OrderID] = row
		return nil
	})
	projection.OnIntegration(p, func(_ context.Context, e paymentCapturedV1, _ broker.Message) error {
		tb.mu.Lock()
		defer tb.mu.Unlock()
		row := tb.rows[e.OrderID]
		row.Status = "paid"
		tb.rows[e.OrderID] = row
		return nil
	})
	return p
}

var fastCfg = projection.Config{PollInterval: 2 * time.Millisecond}

func run(t *testing.T, name string, fn func(ctx context.Context) error) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fn(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("%s = %v, want nil", name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s did not stop", name)
		}
	})
	return cancel
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newRegistry() *eventstore.Registry {
	r := eventstore.NewRegistry()
	eventstore.Register[orderPlaced](r)
	eventstore.Register[orderAudited](r)
	return r
}

func appendEvents(t *testing.T, store eventstore.Store, id string, expected int64, events ...interface {
	EventName() string
}) {
	t.Helper()
	registry := newRegistry()
	data := make([]eventstore.EventData, len(events))
	for i, e := range events {
		var err error
		if data[i], err = registry.Encode(e); err != nil {
			t.Fatal(err)
		}
	}
	stream := eventstore.StreamID{Category: "order", ID: id}
	if err := store.Append(context.Background(), stream, expected, data); err != nil {
		t.Fatal(err)
	}
}

func TestCatchUpProjectsOwnEvents(t *testing.T) {
	tb := newSummaryTable()
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()
	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 5000}, orderAudited{})

	catchup := projection.NewCatchUp(newProjection(tb), store, newRegistry(), cps, fastCfg)
	run(t, "CatchUp", catchup.Run)

	waitFor(t, "row 42", func() bool { r, ok := tb.get("42"); return ok && r.Status == "placed" })
	row, _ := tb.get("42")
	if row.Cents != 5000 {
		t.Errorf("Cents = %d, want 5000", row.Cents)
	}
	if row.Placed.IsZero() {
		t.Error("Meta.OccurredAt not passed through")
	}
	// The unhandled orderAudited must still be checkpointed past.
	waitFor(t, "checkpoint", func() bool {
		seq, _ := cps.Get(context.Background(), "projection.order_summary.store")
		return seq == 2
	})
}

func TestCatchUpResumesWithoutReapplying(t *testing.T) {
	tb := newSummaryTable()
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()
	p := newProjection(tb)

	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 1})
	cancel := run(t, "CatchUp", projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run)
	waitFor(t, "first apply", func() bool { _, ok := tb.get("42"); return ok })
	cancel()

	appendEvents(t, store, "43", 0, orderPlaced{OrderID: "43", Cents: 2})
	run(t, "CatchUp2", projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run)
	waitFor(t, "second apply", func() bool { _, ok := tb.get("43"); return ok })

	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.applied != 2 {
		t.Errorf("applied = %d, want 2 (checkpoint must prevent re-application)", tb.applied)
	}
}

func TestCatchUpHaltsOnHandlerError(t *testing.T) {
	store := eventstore.NewMemoryStore()
	cps := eventstore.NewMemoryCheckpointStore()
	p := projection.New("failing")
	boom := errors.New("read model db down")
	projection.On(p, func(context.Context, orderPlaced, projection.Meta) error { return boom })

	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42"})

	err := projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run = %v, want handler error", err)
	}
	if seq, _ := cps.Get(context.Background(), "projection.failing.store"); seq != 0 {
		t.Errorf("checkpoint = %d, want 0 (failed batch must not advance)", seq)
	}
}

func paidMsg(id, orderID string) broker.Message {
	return broker.Message{
		ID: id, Key: orderID, Type: "billing.payment_captured", Version: 1,
		OccurredAt: time.Now().UTC(), Payload: []byte(`{"order_id":"` + orderID + `"}`),
	}
}

func TestForeignEventsFlowThroughInbox(t *testing.T) {
	tb := newSummaryTable()
	store := eventstore.NewMemoryStore()
	memBroker := broker.NewMemoryBroker()
	inbox := projection.NewMemoryInbox()
	cps := eventstore.NewMemoryCheckpointStore()
	p := newProjection(tb)

	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 100})
	run(t, "CatchUp", projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run)
	run(t, "InboxWriter", projection.NewInboxWriter(memBroker, inbox, "billing.events", "ordering.order_summary").Run)
	run(t, "InboxReader", projection.NewInboxReader(p, inbox, cps, fastCfg).Run)

	// Billing publishes; a redelivery of the same ID must collapse.
	if err := memBroker.Publish(context.Background(), "billing.events",
		paidMsg("payment/9#1", "42"), paidMsg("payment/9#1", "42")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "status paid", func() bool { r, _ := tb.get("42"); return r.Status == "paid" })
	row, _ := tb.get("42")
	if row.Cents != 100 {
		t.Errorf("foreign handler clobbered own data: %+v", row)
	}

	msgs, _ := inbox.ReadAll(context.Background(), 0, 0)
	if len(msgs) != 1 {
		t.Errorf("inbox holds %d messages, want 1 (dedup by ID)", len(msgs))
	}
}

func TestUnregisteredVersionIsSkipped(t *testing.T) {
	tb := newSummaryTable()
	inbox := projection.NewMemoryInbox()
	cps := eventstore.NewMemoryCheckpointStore()
	p := newProjection(tb)

	v2 := paidMsg("payment/9#1", "42")
	v2.Version = 2 // only v1 is registered
	if err := inbox.Append(context.Background(), v2); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Append(context.Background(), paidMsg("payment/9#2", "42")); err != nil {
		t.Fatal(err)
	}

	run(t, "InboxReader", projection.NewInboxReader(p, inbox, cps, fastCfg).Run)
	waitFor(t, "v1 applied", func() bool { r, _ := tb.get("42"); return r.Status == "paid" })
	waitFor(t, "checkpoint past v2", func() bool {
		seq, _ := cps.Get(context.Background(), "projection.order_summary.inbox")
		return seq == 2
	})
}

func TestRebuildReplaysInboxWithoutBroker(t *testing.T) {
	tb := newSummaryTable()
	store := eventstore.NewMemoryStore()
	inbox := projection.NewMemoryInbox()
	cps := eventstore.NewMemoryCheckpointStore()
	p := newProjection(tb)

	appendEvents(t, store, "42", 0, orderPlaced{OrderID: "42", Cents: 100})
	if err := inbox.Append(context.Background(), paidMsg("payment/9#1", "42")); err != nil {
		t.Fatal(err)
	}

	// First build.
	c1 := run(t, "CatchUp", projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run)
	r1 := run(t, "InboxReader", projection.NewInboxReader(p, inbox, cps, fastCfg).Run)
	waitFor(t, "built", func() bool { r, _ := tb.get("42"); return r.Status == "paid" })
	c1()
	r1()

	// Rebuild from scratch: truncate + Reset + restart. No broker involved.
	tb.mu.Lock()
	tb.rows = map[string]summaryRow{}
	tb.mu.Unlock()
	if err := projection.Reset(context.Background(), cps, p); err != nil {
		t.Fatal(err)
	}

	run(t, "CatchUp2", projection.NewCatchUp(p, store, newRegistry(), cps, fastCfg).Run)
	run(t, "InboxReader2", projection.NewInboxReader(p, inbox, cps, fastCfg).Run)
	waitFor(t, "rebuilt", func() bool {
		r, ok := tb.get("42")
		return ok && r.Status == "paid" && r.Cents == 100
	})
}
