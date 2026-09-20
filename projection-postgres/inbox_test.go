package projectionpg_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/akaporn-katip/gohex/broker"
	projectionpg "github.com/akaporn-katip/gohex/projection-postgres"
)

// newCleanInbox returns an empty inbox on a real Postgres, or skips.
func newCleanInbox(t *testing.T) *projectionpg.Inbox {
	t.Helper()
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
	if err := projectionpg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `TRUNCATE inbox RESTART IDENTITY`); err != nil {
		t.Fatal(err)
	}
	return projectionpg.NewInbox(pool)
}

func TestInbox(t *testing.T) {
	ctx := context.Background()
	inbox := newCleanInbox(t)
	msg := broker.Message{
		ID: "payment/9#1", Key: "42", Type: "billing.payment_captured", Version: 1,
		OccurredAt: time.Now().UTC().Truncate(time.Microsecond),
		Payload:    json.RawMessage(`{"order_id":"42"}`),
		Metadata:   map[string]string{"traceparent": "00-abc-def-01"},
	}
	if err := inbox.Append(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Append(ctx, msg); err != nil {
		t.Fatal("duplicate append must be a no-op, got:", err)
	}
	if err := inbox.Append(ctx, broker.Message{ID: "payment/9#2", Type: "x", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	msgs, err := inbox.ReadAll(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("inbox holds %d, want 2 (dedup by id)", len(msgs))
	}
	got := msgs[0].Message
	if got.ID != msg.ID || got.Type != msg.Type || got.Version != msg.Version ||
		got.Metadata["traceparent"] != "00-abc-def-01" {
		t.Errorf("round trip lost data: %+v", got)
	}
	// jsonb normalizes formatting (whitespace, key order), so payloads
	// round-trip semantically, not byte-for-byte — same tolerance as the
	// eventstore contract tests.
	if !jsonEqual(t, got.Payload, msg.Payload) {
		t.Errorf("Payload = %s, want JSON-equal to %s", got.Payload, msg.Payload)
	}
	if !got.OccurredAt.Equal(msg.OccurredAt) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, msg.OccurredAt)
	}

	tail, err := inbox.ReadAll(ctx, msgs[0].Seq, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 1 || tail[0].Message.ID != "payment/9#2" {
		t.Fatalf("tail = %+v", tail)
	}
}

func TestInboxHead(t *testing.T) {
	ctx := context.Background()
	inbox := newCleanInbox(t)

	head, err := inbox.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if head != 0 {
		t.Errorf("Head(empty) = %d, want 0", head)
	}

	msg := broker.Message{ID: "payment/9#1", Type: "x", Payload: json.RawMessage(`{}`)}
	if err := inbox.Append(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if err := inbox.Append(ctx, broker.Message{ID: "payment/9#2", Type: "x", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}

	head, err = inbox.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	msgs, err := inbox.ReadAll(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := msgs[len(msgs)-1].Seq; head != got {
		t.Errorf("Head = %d, last ReadAll seq = %d", head, got)
	}
	tail, err := inbox.ReadAll(ctx, head, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 0 {
		t.Errorf("ReadAll(after head) returned %d messages, want 0", len(tail))
	}

	// A redelivery collapses, so it must not move the head.
	if err := inbox.Append(ctx, msg); err != nil {
		t.Fatal(err)
	}
	after, err := inbox.Head(ctx)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if after != head {
		t.Errorf("Head moved on a duplicate append: %d then %d", head, after)
	}
}

func jsonEqual(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		t.Fatalf("unmarshal %s: %v", a, err)
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		t.Fatalf("unmarshal %s: %v", b, err)
	}
	return reflect.DeepEqual(av, bv)
}
