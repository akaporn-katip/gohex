package eventstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"
)

// MemoryStore is an in-process Store for tests and single-process wiring
// (ADR-0003). It honors the full port contract, including optimistic
// concurrency and global ordering.
type MemoryStore struct {
	mu      sync.Mutex
	all     []RecordedEvent
	streams map[StreamID][]int // indexes into all, in version order
}

var _ Store = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{streams: map[StreamID][]int{}}
}

func (s *MemoryStore) Append(_ context.Context, stream StreamID, expectedVersion int64, events []EventData) error {
	if len(events) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	current := int64(len(s.streams[stream]))
	if current != expectedVersion {
		return fmt.Errorf("%w: stream %s/%s at version %d, expected %d",
			ErrVersionConflict, stream.Category, stream.ID, current, expectedVersion)
	}
	now := time.Now().UTC()
	for i, e := range events {
		rec := RecordedEvent{
			GlobalSeq:     int64(len(s.all)) + 1,
			Stream:        stream,
			Version:       expectedVersion + int64(i) + 1,
			EventName:     e.EventName,
			SchemaVersion: e.SchemaVersion,
			Payload:       slices.Clone(e.Payload),
			Metadata:      cloneMeta(e.Metadata),
			OccurredAt:    now,
		}
		s.streams[stream] = append(s.streams[stream], len(s.all))
		s.all = append(s.all, rec)
	}
	return nil
}

func (s *MemoryStore) Load(_ context.Context, stream StreamID, afterVersion int64) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []RecordedEvent
	for _, idx := range s.streams[stream] {
		if rec := s.all[idx]; rec.Version > afterVersion {
			out = append(out, cloneRec(rec))
		}
	}
	return out, nil
}

func (s *MemoryStore) ReadAll(_ context.Context, afterSeq int64, limit int) ([]RecordedEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var out []RecordedEvent
	for _, rec := range s.all {
		if rec.GlobalSeq <= afterSeq {
			continue
		}
		out = append(out, cloneRec(rec))
		if limit > 0 && len(out) == limit {
			break
		}
	}
	return out, nil
}

func cloneRec(rec RecordedEvent) RecordedEvent {
	rec.Payload = slices.Clone(rec.Payload)
	rec.Metadata = cloneMeta(rec.Metadata)
	return rec
}

func cloneMeta(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	return maps.Clone(m)
}
