package projection_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/projection"
)

func TestWaitForCheckpointAlreadyReached(t *testing.T) {
	ctx := context.Background()
	cps := eventstore.NewMemoryCheckpointStore()
	if err := cps.Set(ctx, "p.store", 7); err != nil {
		t.Fatal(err)
	}
	if err := projection.WaitForCheckpoint(ctx, cps, "p.store", 7); err != nil {
		t.Fatalf("WaitForCheckpoint(reached) = %v, want nil", err)
	}
	if err := projection.WaitForCheckpoint(ctx, cps, "p.store", 3); err != nil {
		t.Fatalf("WaitForCheckpoint(passed) = %v, want nil", err)
	}
}

func TestWaitForCheckpointZeroSeqReturnsImmediately(t *testing.T) {
	cps := eventstore.NewMemoryCheckpointStore()
	if err := projection.WaitForCheckpoint(context.Background(), cps, "p.store", 0); err != nil {
		t.Fatalf("WaitForCheckpoint(0) = %v, want nil", err)
	}
}

func TestWaitForCheckpointUnblocksWhenCheckpointAdvances(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cps := eventstore.NewMemoryCheckpointStore()

	done := make(chan error, 1)
	go func() {
		done <- projection.WaitForCheckpoint(ctx, cps, "p.store", 5,
			projection.WithWaitPollInterval(time.Millisecond))
	}()

	time.Sleep(10 * time.Millisecond)
	if err := cps.Set(ctx, "p.store", 4); err != nil { // not enough yet
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	select {
	case err := <-done:
		t.Fatalf("returned early at checkpoint 4: %v", err)
	default:
	}
	if err := cps.Set(ctx, "p.store", 6); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("WaitForCheckpoint = %v, want nil after checkpoint advanced", err)
	}
}

func TestWaitForCheckpointBoundedByContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	cps := eventstore.NewMemoryCheckpointStore()

	err := projection.WaitForCheckpoint(ctx, cps, "p.store", 99,
		projection.WithWaitPollInterval(time.Millisecond))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitForCheckpoint(timeout) = %v, want DeadlineExceeded", err)
	}
}
