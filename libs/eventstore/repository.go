package eventstore

import (
	"context"
	"fmt"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

// Repository loads and saves event-sourced aggregates of type A (a
// pointer type embedding kernel.Root). It owns the store/registry
// plumbing so application code deals only in aggregates:
//
//	repo := eventstore.NewRepository(store, registry, "order",
//		func() *Order { return &Order{} })
//	o, err := repo.Load(ctx, id)
//	...
//	err = repo.Save(ctx, id, o)
type Repository[A kernel.Aggregate] struct {
	store    Store
	registry *Registry
	category string
	newAgg   func() A
}

func NewRepository[A kernel.Aggregate](store Store, registry *Registry, category string, newAgg func() A) *Repository[A] {
	return &Repository[A]{store: store, registry: registry, category: category, newAgg: newAgg}
}

// Load rehydrates the aggregate identified by id (typically a
// kernel.ID). A stream with no events yields ErrNotFound.
func (r *Repository[A]) Load(ctx context.Context, id interface{ String() string }) (A, error) {
	var zero A
	stream := StreamID{Category: r.category, ID: id.String()}
	recs, err := r.store.Load(ctx, stream, 0)
	if err != nil {
		return zero, err
	}
	if len(recs) == 0 {
		return zero, fmt.Errorf("%w: %s/%s", ErrNotFound, stream.Category, stream.ID)
	}
	events := make([]kernel.DomainEvent, len(recs))
	for i, rec := range recs {
		if events[i], err = r.registry.Decode(rec); err != nil {
			return zero, err
		}
	}
	a := r.newAgg()
	if err := kernel.Rehydrate(a, events...); err != nil {
		return zero, err
	}
	return a, nil
}

// Save appends the aggregate's uncommitted events at its current version
// and, only on success, marks them committed. ErrVersionConflict means a
// concurrent writer won; reload and retry the command. Saving an
// aggregate with no uncommitted events is a no-op.
func (r *Repository[A]) Save(ctx context.Context, id interface{ String() string }, a A) error {
	pending := a.UncommittedEvents()
	if len(pending) == 0 {
		return nil
	}
	data := make([]EventData, len(pending))
	for i, e := range pending {
		var err error
		if data[i], err = r.registry.Encode(e); err != nil {
			return err
		}
	}
	stream := StreamID{Category: r.category, ID: id.String()}
	if err := r.store.Append(ctx, stream, a.Version(), data); err != nil {
		return err
	}
	kernel.TakeUncommitted(a)
	return nil
}
