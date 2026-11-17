package featureflag_test

import (
	"context"
	"strings"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/featureflag"
)

func TestClient_Enabled_withoutRecorder(t *testing.T) {
	src := featureflag.Static{"feature-a": true}
	client := featureflag.NewClient(src)

	// Works even without a recorder on ctx
	subj := featureflag.Subject{Key: "user-123"}
	if !client.Enabled(context.Background(), "feature-a", subj, false) {
		t.Error("feature-a should be enabled")
	}
	if client.Enabled(context.Background(), "feature-b", subj, false) {
		t.Error("feature-b should be disabled (default false)")
	}
}

func TestClient_Enabled_withRecorder(t *testing.T) {
	// Reset metadata extractors to ensure clean state
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })
	featureflag.Install()

	src := featureflag.Static{"feature-a": true, "feature-b": false}
	client := featureflag.NewClient(src)
	subj := featureflag.Subject{Key: "user-123"}

	ctx := featureflag.Record(context.Background())
	client.Enabled(ctx, "feature-a", subj, false) // true
	client.Enabled(ctx, "feature-b", subj, false) // false
	client.Enabled(ctx, "feature-c", subj, true)  // true (default)

	md := eventstore.MetadataFromContext(ctx)
	flags := md[featureflag.FlagsKey]
	// Only feature-a and feature-c evaluated to true
	if !contains(flags, "feature-a") {
		t.Errorf("flags = %q, missing feature-a", flags)
	}
	if !contains(flags, "feature-c") {
		t.Errorf("flags = %q, missing feature-c", flags)
	}
	if contains(flags, "feature-b") {
		t.Errorf("flags = %q, should not contain feature-b (was false)", flags)
	}
}

func TestClient_Enabled_noRecorderMeansNoPinning(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })
	featureflag.Install()

	src := featureflag.Static{"feature-a": true}
	client := featureflag.NewClient(src)
	subj := featureflag.Subject{Key: "user-123"}

	// No Record call, so no recorder on ctx
	ctx := context.Background()
	client.Enabled(ctx, "feature-a", subj, false)

	md := eventstore.MetadataFromContext(ctx)
	if md != nil && md[featureflag.FlagsKey] != "" {
		t.Errorf("flags = %q, want empty when no recorder present", md[featureflag.FlagsKey])
	}
}

func TestPinnedIn_singleFlag(t *testing.T) {
	md := map[string]string{featureflag.FlagsKey: "new-pricing"}
	if !featureflag.PinnedIn(md, "new-pricing") {
		t.Error("new-pricing should be pinned")
	}
	if featureflag.PinnedIn(md, "other-flag") {
		t.Error("other-flag should not be pinned")
	}
}

func TestPinnedIn_multipleFlags(t *testing.T) {
	md := map[string]string{featureflag.FlagsKey: "flag-a,flag-b,flag-c"}
	if !featureflag.PinnedIn(md, "flag-a") {
		t.Error("flag-a should be pinned")
	}
	if !featureflag.PinnedIn(md, "flag-b") {
		t.Error("flag-b should be pinned")
	}
	if !featureflag.PinnedIn(md, "flag-c") {
		t.Error("flag-c should be pinned")
	}
	if featureflag.PinnedIn(md, "flag-d") {
		t.Error("flag-d should not be pinned")
	}
}

func TestPinnedIn_emptyList(t *testing.T) {
	md := map[string]string{featureflag.FlagsKey: ""}
	if featureflag.PinnedIn(md, "any-flag") {
		t.Error("any-flag should not be pinned when list is empty")
	}
}

func TestPinnedIn_keyAbsent(t *testing.T) {
	md := map[string]string{"other-key": "value"}
	if featureflag.PinnedIn(md, "any-flag") {
		t.Error("any-flag should not be pinned when key is absent")
	}
}

func TestPinnedIn_substringDoesNotMatch(t *testing.T) {
	md := map[string]string{featureflag.FlagsKey: "super-new-pricing-v2"}
	// "new-pricing" is a substring but not an exact flag name
	if featureflag.PinnedIn(md, "new-pricing") {
		t.Error("new-pricing should not match super-new-pricing-v2")
	}
}

func TestPinnedIn_commaEdgeCases(t *testing.T) {
	tests := []struct {
		name string
		list string
		flag string
		want bool
	}{
		{"first in list", "flag-a,flag-b", "flag-a", true},
		{"last in list", "flag-a,flag-b", "flag-b", true},
		{"middle of list", "flag-a,flag-b,flag-c", "flag-b", true},
		{"only flag", "flag-a", "flag-a", true},
		{"not in list", "flag-a,flag-b", "flag-c", false},
		{"prefix match rejected", "flag-ab,other", "flag-a", false},
		{"suffix match rejected", "other,aflag-b", "flag-b", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			md := map[string]string{featureflag.FlagsKey: tt.list}
			got := featureflag.PinnedIn(md, tt.flag)
			if got != tt.want {
				t.Errorf("PinnedIn(md, %q) = %v, want %v (list = %q)",
					tt.flag, got, tt.want, tt.list)
			}
		})
	}
}

func TestStatic_Enabled(t *testing.T) {
	src := featureflag.Static{
		"on-flag":  true,
		"off-flag": false,
	}

	subj := featureflag.Subject{Key: "user-123"}
	ctx := context.Background()

	if !src.Enabled(ctx, "on-flag", subj, false) {
		t.Error("on-flag should be true")
	}
	if src.Enabled(ctx, "off-flag", subj, true) {
		t.Error("off-flag should be false")
	}
	// Undefined flag uses default
	if !src.Enabled(ctx, "undefined", subj, true) {
		t.Error("undefined flag should use default true")
	}
	if src.Enabled(ctx, "undefined", subj, false) {
		t.Error("undefined flag should use default false")
	}
}

func TestNoSubject(t *testing.T) {
	if featureflag.NoSubject.Key != "" {
		t.Errorf("NoSubject.Key = %q, want empty", featureflag.NoSubject.Key)
	}
	if featureflag.NoSubject.Attrs != nil {
		t.Errorf("NoSubject.Attrs = %v, want nil", featureflag.NoSubject.Attrs)
	}
}

func TestRecord_roundTrip(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })
	featureflag.Install()

	src := featureflag.Static{"flag-x": true, "flag-y": true}
	client := featureflag.NewClient(src)
	subj := featureflag.Subject{Key: "tenant-456", Attrs: map[string]string{"region": "us-west"}}

	ctx := featureflag.Record(context.Background())
	client.Enabled(ctx, "flag-x", subj, false)
	client.Enabled(ctx, "flag-y", subj, false)

	md := eventstore.MetadataFromContext(ctx)
	// Now read them back
	if !featureflag.PinnedIn(md, "flag-x") {
		t.Error("flag-x should round-trip through metadata")
	}
	if !featureflag.PinnedIn(md, "flag-y") {
		t.Error("flag-y should round-trip through metadata")
	}
}

func contains(list, flag string) bool {
	if list == "" {
		return false
	}
	parts := strings.Split(list, ",")
	for _, p := range parts {
		if p == flag {
			return true
		}
	}
	return false
}
