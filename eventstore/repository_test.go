package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
)

// Minimal event-sourced aggregate for repository tests.

type counter struct {
	kernel.Root
	id    kernel.ID[counter]
	total int
}

type counterBumped struct {
	ID kernel.ID[counter] `json:"id"`
	By int                `json:"by"`
}

func (counterBumped) EventName() string { return "test.counter_bumped" }

func (c *counter) Bump(by int) error {
	if by <= 0 {
		return kernel.NewDomainError("bad_bump", "bump must be positive")
	}
	return kernel.Raise(c, counterBumped{ID: c.id, By: by})
}

func (c *counter) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case counterBumped:
		c.id = ev.ID
		c.total += ev.By
	default:
		return fmt.Errorf("counter: unknown event %q", e.EventName())
	}
	return nil
}

func newCounterRepo() (*eventstore.Repository[*counter], *eventstore.MemoryStore) {
	store := eventstore.NewMemoryStore()
	registry := eventstore.NewRegistry()
	eventstore.Register[counterBumped](registry)
	repo := eventstore.NewRepository(store, registry, "counter",
		func() *counter { return &counter{} })
	return repo, store
}

func TestRepositorySaveLoadRoundTrip(t *testing.T) {
	ctx := context.Background()
	repo, _ := newCounterRepo()
	id := kernel.NewID[counter]()

	c := &counter{id: id}
	if err := c.Bump(2); err != nil {
		t.Fatal(err)
	}
	if err := c.Bump(3); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(ctx, id, c); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if got := c.Version(); got != 2 {
		t.Errorf("Version after save = %d, want 2", got)
	}
	if got := len(c.UncommittedEvents()); got != 0 {
		t.Errorf("uncommitted after save = %d, want 0", got)
	}

	loaded, err := repo.Load(ctx, id)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.total != 5 || loaded.id != id {
		t.Errorf("loaded = %+v, want total 5", loaded)
	}
	if got := loaded.Version(); got != 2 {
		t.Errorf("loaded Version = %d, want 2", got)
	}
}

func TestRepositoryLoadNotFound(t *testing.T) {
	repo, _ := newCounterRepo()
	_, err := repo.Load(context.Background(), kernel.NewID[counter]())
	if !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatalf("Load(missing) = %v, want ErrNotFound", err)
	}
	if _, ok := kernel.AsDomainError(err); !ok {
		t.Error("ErrNotFound must be a DomainError (definitive rejection)")
	}
}

func TestRepositoryStaleSaveConflicts(t *testing.T) {
	ctx := context.Background()
	repo, _ := newCounterRepo()
	id := kernel.NewID[counter]()

	c := &counter{id: id}
	_ = c.Bump(1)
	if err := repo.Save(ctx, id, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Two handlers load the same version; the slower one must conflict.
	first, _ := repo.Load(ctx, id)
	second, _ := repo.Load(ctx, id)
	_ = first.Bump(1)
	if err := repo.Save(ctx, id, first); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	_ = second.Bump(1)
	err := repo.Save(ctx, id, second)
	if !errors.Is(err, eventstore.ErrVersionConflict) {
		t.Fatalf("stale Save = %v, want ErrVersionConflict", err)
	}
	if _, ok := kernel.AsDomainError(err); ok {
		t.Error("ErrVersionConflict must NOT be a DomainError (it is retryable)")
	}
	if got := len(second.UncommittedEvents()); got != 1 {
		t.Errorf("failed save must keep events pending: %d, want 1", got)
	}
}

func TestRepositorySaveNothingIsNoOp(t *testing.T) {
	ctx := context.Background()
	repo, store := newCounterRepo()
	id := kernel.NewID[counter]()

	if err := repo.Save(ctx, id, &counter{}); err != nil {
		t.Fatalf("Save(clean) = %v", err)
	}
	recs, _ := store.ReadAll(ctx, 0, 0)
	if len(recs) != 0 {
		t.Errorf("no-op save stored %d events", len(recs))
	}
}
