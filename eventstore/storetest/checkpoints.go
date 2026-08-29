package storetest

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
)

// RunCheckpoints exercises the CheckpointStore contract. newStore must
// return an empty store; it is called once per subtest.
func RunCheckpoints(t *testing.T, newStore func(t *testing.T) eventstore.CheckpointStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("unknown checkpoint is zero", func(t *testing.T) {
		s := newStore(t)
		seq, err := s.Get(ctx, "ghost")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if seq != 0 {
			t.Fatalf("Get(unknown) = %d, want 0", seq)
		}
	})

	t.Run("set then get", func(t *testing.T) {
		s := newStore(t)
		if err := s.Set(ctx, "relay", 42); err != nil {
			t.Fatalf("Set: %v", err)
		}
		seq, err := s.Get(ctx, "relay")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if seq != 42 {
			t.Fatalf("Get = %d, want 42", seq)
		}
	})

	t.Run("set overwrites", func(t *testing.T) {
		s := newStore(t)
		mustSet(t, s, "relay", 1)
		mustSet(t, s, "relay", 7)
		if seq, _ := s.Get(ctx, "relay"); seq != 7 {
			t.Fatalf("Get = %d, want 7", seq)
		}
	})

	t.Run("names are independent", func(t *testing.T) {
		s := newStore(t)
		mustSet(t, s, "relay", 3)
		mustSet(t, s, "projection.order_summary", 9)
		if seq, _ := s.Get(ctx, "relay"); seq != 3 {
			t.Fatalf("relay = %d, want 3", seq)
		}
		if seq, _ := s.Get(ctx, "projection.order_summary"); seq != 9 {
			t.Fatalf("projection = %d, want 9", seq)
		}
	})
}

func mustSet(t *testing.T, s eventstore.CheckpointStore, name string, seq int64) {
	t.Helper()
	if err := s.Set(context.Background(), name, seq); err != nil {
		t.Fatalf("Set(%s, %d): %v", name, seq, err)
	}
}
