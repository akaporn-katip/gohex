package o11y

import (
	"testing"

	sdkresource "go.opentelemetry.io/otel/sdk/resource"
)

// TestBuildResourceNoSchemaConflict verifies that buildResource never
// produces conflicting schema URLs regardless of what SDK version is
// resolved. The regression: semconv.SchemaURL (v1.26.0) conflicted with
// sdkresource.Default()'s schema (v1.43.0 under SDK 1.47), and Merge
// refused the mismatch. NewSchemaless keeps Default()'s schema and never
// conflicts.
func TestBuildResourceNoSchemaConflict(t *testing.T) {
	res, err := buildResource("test-service")
	if err != nil {
		t.Fatalf("buildResource failed: %v (this broke billing startup under SDK 1.47)", err)
	}

	// The service name must be present.
	for _, attr := range res.Attributes() {
		if string(attr.Key) == "service.name" && attr.Value.AsString() == "test-service" {
			return
		}
	}
	t.Error("service.name not found in resource attributes")
}

// TestBuildResourcePreservesDefaultSchema verifies that the merged
// resource carries sdkresource.Default()'s schema URL, not an empty one
// and not semconv's older version.
func TestBuildResourcePreservesDefaultSchema(t *testing.T) {
	res, err := buildResource("test-service")
	if err != nil {
		t.Fatalf("buildResource failed: %v", err)
	}

	defaultRes := sdkresource.Default()
	if res.SchemaURL() != defaultRes.SchemaURL() {
		t.Errorf("merged resource schema = %q, want Default()'s %q (NewSchemaless must not clobber it)",
			res.SchemaURL(), defaultRes.SchemaURL())
	}
}
