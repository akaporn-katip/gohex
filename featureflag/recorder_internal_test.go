package featureflag

import (
	"context"
	"testing"
)

// TestRecordDedupesRepeatedChecks guards the pinned list against a flag
// checked more than once in a single request: the list is a set.
func TestRecordDedupesRepeatedChecks(t *testing.T) {
	c := NewClient(Static{"new-pricing": true})
	ctx := Record(context.Background())
	for range 3 {
		c.Enabled(ctx, "new-pricing", NoSubject, false)
	}
	if got := recorderFromContext(ctx).snapshot(); len(got) != 1 || got[0] != "new-pricing" {
		t.Errorf("snapshot() = %v, want [new-pricing]", got)
	}
}
