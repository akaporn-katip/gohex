package eventstore_test

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
)

func TestCapturePositionRecordsSavePosition(t *testing.T) {
	ctx, pos := eventstore.CapturePosition(context.Background())
	repo, store := newCounterRepo()
	id := kernel.NewID[counter]()

	if got := pos.Seq(); got != 0 {
		t.Errorf("Seq before any save = %d, want 0", got)
	}

	c := &counter{id: id}
	_ = c.Bump(1)
	_ = c.Bump(2)
	if err := repo.Save(ctx, id, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	recs, _ := store.ReadAll(ctx, 0, 0)
	want := recs[len(recs)-1].GlobalSeq
	if got := pos.Seq(); got != want {
		t.Errorf("Seq = %d, want last appended GlobalSeq %d", got, want)
	}
}

func TestCapturePositionKeepsMaxAcrossSaves(t *testing.T) {
	ctx, pos := eventstore.CapturePosition(context.Background())
	repo, store := newCounterRepo()

	for range 2 { // a handler may save several aggregates
		id := kernel.NewID[counter]()
		c := &counter{id: id}
		_ = c.Bump(1)
		if err := repo.Save(ctx, id, c); err != nil {
			t.Fatalf("Save: %v", err)
		}
	}

	recs, _ := store.ReadAll(ctx, 0, 0)
	want := recs[len(recs)-1].GlobalSeq
	if got := pos.Seq(); got != want {
		t.Errorf("Seq = %d, want max GlobalSeq %d", got, want)
	}
}

func TestCapturePositionUntouchedByFailedSave(t *testing.T) {
	ctx, pos := eventstore.CapturePosition(context.Background())
	repo, _ := newCounterRepo()
	id := kernel.NewID[counter]()

	c := &counter{id: id}
	_ = c.Bump(1)
	if err := repo.Save(ctx, id, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	after := pos.Seq()

	stale, _ := repo.Load(ctx, id)
	fresh, _ := repo.Load(ctx, id)
	_ = fresh.Bump(1)
	if err := repo.Save(context.Background(), id, fresh); err != nil { // outside the capture
		t.Fatalf("fresh Save: %v", err)
	}
	_ = stale.Bump(1)
	if err := repo.Save(ctx, id, stale); err == nil {
		t.Fatal("stale Save must conflict")
	}
	if got := pos.Seq(); got != after {
		t.Errorf("Seq after failed save = %d, want unchanged %d", got, after)
	}
}

func TestSaveWithoutCapturedPositionIsFine(t *testing.T) {
	repo, _ := newCounterRepo()
	id := kernel.NewID[counter]()
	c := &counter{id: id}
	_ = c.Bump(1)
	if err := repo.Save(context.Background(), id, c); err != nil {
		t.Fatalf("Save without capture: %v", err)
	}
}

func TestCapturePositionShadowsOuter(t *testing.T) {
	outerCtx, outer := eventstore.CapturePosition(context.Background())
	innerCtx, inner := eventstore.CapturePosition(outerCtx)
	repo, _ := newCounterRepo()
	id := kernel.NewID[counter]()

	c := &counter{id: id}
	_ = c.Bump(1)
	if err := repo.Save(innerCtx, id, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if inner.Seq() == 0 {
		t.Error("inner position missed the save")
	}
	if outer.Seq() != 0 {
		t.Error("outer position must be shadowed by the inner capture")
	}
}
