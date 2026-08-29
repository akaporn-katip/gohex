package eventstore_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
)

type thingCreated struct {
	Name string `json:"name"`
}

func (thingCreated) EventName() string { return "test.thing_created" }

type nameless struct{}

func (nameless) EventName() string { return "" }

func TestEncodeDecodeRoundTrip(t *testing.T) {
	r := eventstore.NewRegistry()
	eventstore.Register[thingCreated](r)

	data, err := r.Encode(thingCreated{Name: "widget"})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if data.EventName != "test.thing_created" || data.SchemaVersion != 1 {
		t.Errorf("EventData = %+v", data)
	}

	decoded, err := r.Decode(eventstore.RecordedEvent{
		EventName:     data.EventName,
		SchemaVersion: data.SchemaVersion,
		Payload:       data.Payload,
	})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got := decoded.(thingCreated); got.Name != "widget" {
		t.Errorf("decoded = %+v", got)
	}
}

func TestUnregisteredEvent(t *testing.T) {
	r := eventstore.NewRegistry()
	if _, err := r.Encode(thingCreated{}); err == nil {
		t.Error("Encode(unregistered) = nil error")
	}
	if _, err := r.Decode(eventstore.RecordedEvent{EventName: "test.ghost", SchemaVersion: 1}); err == nil {
		t.Error("Decode(unregistered) = nil error")
	}
}

func TestRegisterPanics(t *testing.T) {
	r := eventstore.NewRegistry()
	eventstore.Register[thingCreated](r)

	assertPanics(t, "duplicate", func() { eventstore.Register[thingCreated](r) })
	assertPanics(t, "empty name", func() { eventstore.Register[nameless](r) })
	assertPanics(t, "upcaster for unknown", func() {
		r.RegisterUpcaster("test.ghost", func(p json.RawMessage) (json.RawMessage, error) { return p, nil })
	})
}

func assertPanics(t *testing.T, name string, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: no panic", name)
		}
	}()
	fn()
}

// TestUpcasterChain simulates two schema changes to thing_created:
// v1 {"title": ...} -> v2 {"name": ...} -> v3 adds {"kind": "unknown"}.
type thingCreatedV3 struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
}

func (thingCreatedV3) EventName() string { return "test.thing_created_v3" }

func TestUpcasterChain(t *testing.T) {
	r := eventstore.NewRegistry()
	eventstore.Register[thingCreatedV3](r)
	r.RegisterUpcaster("test.thing_created_v3", func(p json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(strings.Replace(string(p), `"title"`, `"name"`, 1)), nil
	})
	r.RegisterUpcaster("test.thing_created_v3", func(p json.RawMessage) (json.RawMessage, error) {
		var m map[string]any
		if err := json.Unmarshal(p, &m); err != nil {
			return nil, err
		}
		m["kind"] = "unknown"
		return json.Marshal(m)
	})

	if v, _ := r.SchemaVersion("test.thing_created_v3"); v != 3 {
		t.Fatalf("SchemaVersion = %d, want 3", v)
	}

	cases := []struct {
		stored  int
		payload string
	}{
		{1, `{"title":"widget"}`},          // full chain
		{2, `{"name":"widget"}`},           // half chain
		{3, `{"name":"widget","kind":""}`}, // no chain; kind stays as stored
	}
	for _, c := range cases {
		decoded, err := r.Decode(eventstore.RecordedEvent{
			EventName:     "test.thing_created_v3",
			SchemaVersion: c.stored,
			Payload:       []byte(c.payload),
		})
		if err != nil {
			t.Fatalf("Decode(v%d): %v", c.stored, err)
		}
		got := decoded.(thingCreatedV3)
		if got.Name != "widget" {
			t.Errorf("v%d: Name = %q, want widget", c.stored, got.Name)
		}
		if c.stored < 3 && got.Kind != "unknown" {
			t.Errorf("v%d: Kind = %q, want unknown", c.stored, got.Kind)
		}
	}
}

func TestDecodeRejectsBadStoredVersion(t *testing.T) {
	r := eventstore.NewRegistry()
	eventstore.Register[thingCreated](r)

	for _, v := range []int{0, 2} {
		_, err := r.Decode(eventstore.RecordedEvent{
			EventName:     "test.thing_created",
			SchemaVersion: v,
			Payload:       []byte(`{}`),
		})
		if err == nil {
			t.Errorf("Decode(stored v%d) = nil error, want out-of-range", v)
		}
	}
}
