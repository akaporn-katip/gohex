// Package storetest is the reusable contract-test suite for
// eventstore.Store implementations. Every adapter — including the
// built-in MemoryStore — must pass it, so the port's semantics are
// pinned by tests rather than prose:
//
//	func TestMyStore(t *testing.T) {
//		storetest.Run(t, func(t *testing.T) eventstore.Store {
//			return newCleanStore(t) // fresh/empty store per subtest
//		})
//	}
package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
)

// Run exercises the Store contract. newStore must return an empty store;
// it is called once per subtest.
func Run(t *testing.T, newStore func(t *testing.T) eventstore.Store) {
	t.Helper()
	ctx := context.Background()

	stream := eventstore.StreamID{Category: "order", ID: "42"}
	other := eventstore.StreamID{Category: "order", ID: "43"}

	ev := func(name string) eventstore.EventData {
		return eventstore.EventData{EventName: name, SchemaVersion: 1, Payload: []byte(`{"n":1}`)}
	}

	t.Run("append then load", func(t *testing.T) {
		s := newStore(t)
		if err := s.Append(ctx, stream, 0, []eventstore.EventData{ev("a"), ev("b")}); err != nil {
			t.Fatalf("Append: %v", err)
		}
		recs, err := s.Load(ctx, stream, 0)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(recs) != 2 {
			t.Fatalf("Load len = %d, want 2", len(recs))
		}
		for i, rec := range recs {
			if rec.Version != int64(i)+1 {
				t.Errorf("recs[%d].Version = %d, want %d", i, rec.Version, i+1)
			}
			if rec.Stream != stream {
				t.Errorf("recs[%d].Stream = %v", i, rec.Stream)
			}
			if rec.OccurredAt.IsZero() {
				t.Errorf("recs[%d].OccurredAt is zero", i)
			}
		}
		if recs[0].EventName != "a" || recs[1].EventName != "b" {
			t.Errorf("order: got %s, %s", recs[0].EventName, recs[1].EventName)
		}
		if recs[0].SchemaVersion != 1 {
			t.Errorf("SchemaVersion = %d, want 1", recs[0].SchemaVersion)
		}
		if string(recs[0].Payload) != `{"n":1}` && string(recs[0].Payload) != `{"n": 1}` {
			t.Errorf("Payload = %s", recs[0].Payload)
		}
	})

	t.Run("append continues version across calls", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"))
		mustAppend(t, s, stream, 1, ev("b"))
		recs, _ := s.Load(ctx, stream, 0)
		if len(recs) != 2 || recs[1].Version != 2 {
			t.Fatalf("got %d recs, last version %d; want 2 and 2", len(recs), recs[len(recs)-1].Version)
		}
	})

	t.Run("stale append conflicts", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"))
		err := s.Append(ctx, stream, 0, []eventstore.EventData{ev("b")})
		if !errors.Is(err, eventstore.ErrVersionConflict) {
			t.Fatalf("stale append = %v, want ErrVersionConflict", err)
		}
		recs, _ := s.Load(ctx, stream, 0)
		if len(recs) != 1 {
			t.Errorf("conflicting append must store nothing: len = %d, want 1", len(recs))
		}
	})

	t.Run("concurrent create conflicts", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"))
		if err := s.Append(ctx, stream, 0, []eventstore.EventData{ev("a")}); !errors.Is(err, eventstore.ErrVersionConflict) {
			t.Fatalf("second create = %v, want ErrVersionConflict", err)
		}
	})

	t.Run("future expected version conflicts", func(t *testing.T) {
		s := newStore(t)
		if err := s.Append(ctx, stream, 5, []eventstore.EventData{ev("a")}); !errors.Is(err, eventstore.ErrVersionConflict) {
			t.Fatalf("future version append = %v, want ErrVersionConflict", err)
		}
	})

	t.Run("load after version", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"), ev("b"), ev("c"))
		recs, err := s.Load(ctx, stream, 2)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(recs) != 1 || recs[0].EventName != "c" {
			t.Fatalf("Load(after 2) = %+v, want just c", recs)
		}
	})

	t.Run("missing stream loads empty", func(t *testing.T) {
		s := newStore(t)
		recs, err := s.Load(ctx, eventstore.StreamID{Category: "ghost", ID: "0"}, 0)
		if err != nil {
			t.Fatalf("Load(missing) error = %v, want nil", err)
		}
		if len(recs) != 0 {
			t.Fatalf("Load(missing) len = %d, want 0", len(recs))
		}
	})

	t.Run("streams are isolated", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"))
		mustAppend(t, s, other, 0, ev("b"))
		recs, _ := s.Load(ctx, stream, 0)
		if len(recs) != 1 || recs[0].EventName != "a" {
			t.Fatalf("stream leaked: %+v", recs)
		}
	})

	t.Run("readall tails in global order", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"))
		mustAppend(t, s, other, 0, ev("b"))
		mustAppend(t, s, stream, 1, ev("c"))

		recs, err := s.ReadAll(ctx, 0, 0)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(recs) != 3 {
			t.Fatalf("ReadAll len = %d, want 3", len(recs))
		}
		var last int64
		for i, rec := range recs {
			if rec.GlobalSeq <= last {
				t.Fatalf("GlobalSeq not strictly increasing at %d: %+v", i, recs)
			}
			last = rec.GlobalSeq
		}
		if recs[0].EventName != "a" || recs[1].EventName != "b" || recs[2].EventName != "c" {
			t.Errorf("global order: %s, %s, %s", recs[0].EventName, recs[1].EventName, recs[2].EventName)
		}

		// Tail from a checkpoint: seq of the second event.
		tail, err := s.ReadAll(ctx, recs[1].GlobalSeq, 0)
		if err != nil {
			t.Fatalf("ReadAll(after): %v", err)
		}
		if len(tail) != 1 || tail[0].EventName != "c" {
			t.Fatalf("ReadAll(after 2nd) = %+v, want just c", tail)
		}
	})

	t.Run("readall honors limit", func(t *testing.T) {
		s := newStore(t)
		mustAppend(t, s, stream, 0, ev("a"), ev("b"), ev("c"))
		recs, err := s.ReadAll(ctx, 0, 2)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(recs) != 2 {
			t.Fatalf("ReadAll(limit 2) len = %d, want 2", len(recs))
		}
	})

	t.Run("metadata round trips", func(t *testing.T) {
		s := newStore(t)
		e := ev("a")
		e.Metadata = map[string]string{"traceparent": "00-abc-def-01"}
		mustAppend(t, s, stream, 0, e)
		recs, _ := s.Load(ctx, stream, 0)
		if got := recs[0].Metadata["traceparent"]; got != "00-abc-def-01" {
			t.Errorf("Metadata[traceparent] = %q", got)
		}
	})

	t.Run("empty append is a no-op", func(t *testing.T) {
		s := newStore(t)
		if err := s.Append(ctx, stream, 0, nil); err != nil {
			t.Fatalf("empty Append: %v", err)
		}
		recs, _ := s.ReadAll(ctx, 0, 0)
		if len(recs) != 0 {
			t.Errorf("empty append stored %d events", len(recs))
		}
	})
}

func mustAppend(t *testing.T, s eventstore.Store, stream eventstore.StreamID, expected int64, events ...eventstore.EventData) {
	t.Helper()
	if err := s.Append(context.Background(), stream, expected, events); err != nil {
		t.Fatalf("Append(%s/%s @%d): %v", stream.Category, stream.ID, expected, err)
	}
}
