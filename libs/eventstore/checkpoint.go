package eventstore

import (
	"context"
	"sync"
)

// CheckpointStore persists named cursors on the store's global sequence.
// The relay and every projection worker record how far they have
// processed (ADR-0003, ADR-0006); progress is "process, then advance",
// so a crash replays at-least-once rather than skipping.
type CheckpointStore interface {
	// Get returns the stored position for name, or 0 if none exists.
	Get(ctx context.Context, name string) (int64, error)
	// Set durably records the position for name.
	Set(ctx context.Context, name string, seq int64) error
}

// MemoryCheckpointStore is an in-process CheckpointStore for tests and
// single-process wiring.
type MemoryCheckpointStore struct {
	mu        sync.Mutex
	positions map[string]int64
}

var _ CheckpointStore = (*MemoryCheckpointStore)(nil)

func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{positions: map[string]int64{}}
}

func (s *MemoryCheckpointStore) Get(_ context.Context, name string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.positions[name], nil
}

func (s *MemoryCheckpointStore) Set(_ context.Context, name string, seq int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.positions[name] = seq
	return nil
}
