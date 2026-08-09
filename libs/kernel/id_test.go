package kernel_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

type widget struct{}

type widgetID = kernel.ID[widget]

func TestNewIDRoundTrip(t *testing.T) {
	id := kernel.NewID[widget]()
	if id.IsZero() {
		t.Fatal("NewID returned zero ID")
	}

	parsed, err := kernel.ParseID[widget](id.String())
	if err != nil {
		t.Fatalf("ParseID(%q): %v", id.String(), err)
	}
	if parsed != id {
		t.Errorf("round trip: %v != %v", parsed, id)
	}
}

func TestNewIDIsCanonicalV4(t *testing.T) {
	s := kernel.NewID[widget]().String()
	if len(s) != 36 {
		t.Fatalf("len = %d, want 36: %q", len(s), s)
	}
	if s != strings.ToLower(s) {
		t.Errorf("not lowercase: %q", s)
	}
	if s[14] != '4' {
		t.Errorf("version nibble = %c, want 4: %q", s[14], s)
	}
	if c := s[19]; c != '8' && c != '9' && c != 'a' && c != 'b' {
		t.Errorf("variant nibble = %c, want 8/9/a/b: %q", c, s)
	}
}

func TestNewIDsAreUnique(t *testing.T) {
	seen := make(map[widgetID]bool)
	for range 1000 {
		id := kernel.NewID[widget]()
		if seen[id] {
			t.Fatalf("duplicate ID generated: %v", id)
		}
		seen[id] = true
	}
}

func TestParseIDNormalizesCase(t *testing.T) {
	upper := strings.ToUpper(kernel.NewID[widget]().String())
	id, err := kernel.ParseID[widget](upper)
	if err != nil {
		t.Fatalf("ParseID(%q): %v", upper, err)
	}
	if id.String() != strings.ToLower(upper) {
		t.Errorf("not normalized: %q", id.String())
	}
}

func TestParseIDRejectsMalformed(t *testing.T) {
	cases := []string{
		"",
		"not-a-uuid",
		"3b241101-e2bb-4255-8caf-4136c566a96",   // 35 chars
		"3b241101-e2bb-4255-8caf-4136c566a9622", // 37 chars
		"3b241101xe2bb-4255-8caf-4136c566a962",  // wrong separator
		"3b241101-e2bb-4255-8caf-4136c566a96g",  // non-hex
	}
	for _, in := range cases {
		_, err := kernel.ParseID[widget](in)
		if err == nil {
			t.Errorf("ParseID(%q) = nil error, want ErrInvalidID", in)
			continue
		}
		if !errors.Is(err, kernel.ErrInvalidID) {
			t.Errorf("ParseID(%q) error = %v, not ErrInvalidID", in, err)
		}
		if _, ok := kernel.AsDomainError(err); !ok {
			t.Errorf("ParseID(%q) error is not a DomainError", in)
		}
	}
}

func TestIDJSON(t *testing.T) {
	type doc struct {
		ID widgetID `json:"id"`
	}
	id := kernel.NewID[widget]()

	data, err := json.Marshal(doc{ID: id})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"id":"` + id.String() + `"}`
	if string(data) != want {
		t.Errorf("marshal = %s, want %s", data, want)
	}

	var out doc
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.ID != id {
		t.Errorf("unmarshal = %v, want %v", out.ID, id)
	}

	if err := json.Unmarshal([]byte(`{"id":"garbage"}`), &out); err == nil {
		t.Error("unmarshal of malformed ID succeeded, want error")
	}
}

func TestMarshalZeroIDFails(t *testing.T) {
	var zero widgetID
	if _, err := zero.MarshalText(); err == nil {
		t.Error("MarshalText on zero ID = nil error, want ErrInvalidID")
	}
	if _, err := json.Marshal(struct{ ID widgetID }{}); err == nil {
		t.Error("json.Marshal of zero ID succeeded, want error")
	}
}
