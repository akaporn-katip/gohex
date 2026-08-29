package eventstore

import (
	"encoding/json"
	"fmt"

	"github.com/akaporn-katip/gohex/kernel"
)

// Upcaster lifts a stored payload one schema version (ADR-0010): stored
// payloads are immutable, so shape changes are absorbed at
// deserialization by a pure payload-to-payload function.
type Upcaster func(payload json.RawMessage) (json.RawMessage, error)

type registryEntry struct {
	decode    func([]byte) (kernel.DomainEvent, error)
	upcasters []Upcaster
}

// Registry maps stored event names to Go event types and holds each
// type's upcaster chain. Configure it once at startup (registration
// panics on programmer error) and share it read-only afterwards.
type Registry struct {
	entries map[string]*registryEntry
}

func NewRegistry() *Registry {
	return &Registry{entries: map[string]*registryEntry{}}
}

// Register registers domain event type E under its EventName. E must be
// a struct with a value-receiver EventName method (the kernel
// convention). Panics on a duplicate or empty name — a wiring bug, not a
// runtime condition.
func Register[E kernel.DomainEvent](r *Registry) {
	var zero E
	name := zero.EventName()
	if name == "" {
		panic("eventstore: Register: empty event name")
	}
	if _, dup := r.entries[name]; dup {
		panic(fmt.Sprintf("eventstore: Register: duplicate event name %q", name))
	}
	r.entries[name] = &registryEntry{
		decode: func(payload []byte) (kernel.DomainEvent, error) {
			var e E
			if err := json.Unmarshal(payload, &e); err != nil {
				return nil, fmt.Errorf("eventstore: decoding %s: %w", name, err)
			}
			return e, nil
		},
	}
}

// RegisterUpcaster appends the next upcaster in name's chain. The chain
// defines the schema version: with no upcasters the current version is 1;
// the first registered upcaster lifts v1 payloads to v2 (making the
// current version 2), and so on. Panics if the event is not registered.
func (r *Registry) RegisterUpcaster(name string, up Upcaster) {
	ent, ok := r.entries[name]
	if !ok {
		panic(fmt.Sprintf("eventstore: RegisterUpcaster: unknown event %q", name))
	}
	ent.upcasters = append(ent.upcasters, up)
}

// SchemaVersion returns the current schema version for name: 1 plus the
// length of its upcaster chain.
func (r *Registry) SchemaVersion(name string) (int, error) {
	ent, ok := r.entries[name]
	if !ok {
		return 0, fmt.Errorf("eventstore: unregistered event %q", name)
	}
	return 1 + len(ent.upcasters), nil
}

// Encode serializes a domain event to EventData stamped with the current
// schema version. Metadata is left empty for the caller to fill.
func (r *Registry) Encode(e kernel.DomainEvent) (EventData, error) {
	name := e.EventName()
	version, err := r.SchemaVersion(name)
	if err != nil {
		return EventData{}, err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return EventData{}, fmt.Errorf("eventstore: encoding %s: %w", name, err)
	}
	return EventData{EventName: name, SchemaVersion: version, Payload: payload}, nil
}

// Decode deserializes a stored event, first lifting its payload through
// the upcaster chain from its stored schema version to the current one.
func (r *Registry) Decode(rec RecordedEvent) (kernel.DomainEvent, error) {
	ent, ok := r.entries[rec.EventName]
	if !ok {
		return nil, fmt.Errorf("eventstore: unregistered event %q at seq %d", rec.EventName, rec.GlobalSeq)
	}
	current := 1 + len(ent.upcasters)
	if rec.SchemaVersion < 1 || rec.SchemaVersion > current {
		return nil, fmt.Errorf("eventstore: %s: stored schema version %d outside [1,%d]",
			rec.EventName, rec.SchemaVersion, current)
	}
	payload := json.RawMessage(rec.Payload)
	for v := rec.SchemaVersion; v < current; v++ {
		lifted, err := ent.upcasters[v-1](payload)
		if err != nil {
			return nil, fmt.Errorf("eventstore: upcasting %s v%d->v%d: %w", rec.EventName, v, v+1, err)
		}
		payload = lifted
	}
	return ent.decode(payload)
}
