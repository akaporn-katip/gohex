package o11y_test

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/akaporn-katip/gohex/featureflag"
	"github.com/akaporn-katip/gohex/o11y"
)

// TestObserveFeatureFlagsCountsPerFlagAndAnswer: the series a team reads
// to decide a flag is finished — one per flag and outcome.
func TestObserveFeatureFlagsCountsPerFlagAndAnswer(t *testing.T) {
	reader := meterProvider(t)
	src := o11y.ObserveFeatureFlags(featureflag.Static{
		"express-shipping": true,
		"new-pricing":      false,
	})
	ctx := context.Background()

	src.Enabled(ctx, "express-shipping", featureflag.NoSubject, false)
	src.Enabled(ctx, "express-shipping", featureflag.NoSubject, false)
	src.Enabled(ctx, "new-pricing", featureflag.NoSubject, false)

	points, ok := collectSums(t, reader)["gohex.featureflag.evaluations"]
	if !ok {
		t.Fatalf("gohex.featureflag.evaluations was never collected")
	}
	got := map[string]int64{}
	for _, p := range points {
		name, hasName := p.attrs.Value("gohex.featureflag.name")
		enabled, hasAnswer := p.attrs.Value("gohex.featureflag.enabled")
		if !hasName || !hasAnswer {
			t.Fatalf("point missing name or answer: %v", p.attrs.Encoded(attribute.DefaultEncoder()))
		}
		if p.attrs.Len() != 2 {
			t.Errorf("the point carries the flag and the answer and nothing else, got %v",
				p.attrs.Encoded(attribute.DefaultEncoder()))
		}
		got[name.AsString()+"/"+enabled.Emit()] = p.value
	}
	if got["express-shipping/true"] != 2 || got["new-pricing/false"] != 1 {
		t.Errorf("evaluations = %v, want express-shipping/true 2 and new-pricing/false 1", got)
	}
}

// TestObserveFeatureFlagsCarriesNoSubject: a subject key is a customer
// identifier. It must never reach a metric, whatever the caller passes.
func TestObserveFeatureFlagsCarriesNoSubject(t *testing.T) {
	reader := meterProvider(t)
	src := o11y.ObserveFeatureFlags(featureflag.Static{"express-shipping": true})

	src.Enabled(context.Background(), "express-shipping", featureflag.Subject{
		Key:   "customer-42",
		Attrs: map[string]string{"email": "someone@example.com"},
	}, false)

	for _, p := range collectSums(t, reader)["gohex.featureflag.evaluations"] {
		encoded := p.attrs.Encoded(attribute.DefaultEncoder())
		for _, leak := range []string{"customer-42", "someone@example.com"} {
			if strings.Contains(encoded, leak) {
				t.Errorf("attributes leaked subject data %q: %s", leak, encoded)
			}
		}
	}
}

// TestObserveFeatureFlagsWithoutMeterProviderIsSilent: wired
// unconditionally in a composition root, including on a laptop with no
// collector.
func TestObserveFeatureFlagsWithoutMeterProviderIsSilent(t *testing.T) {
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
	t.Cleanup(func() { otel.SetMeterProvider(metricnoop.NewMeterProvider()) })

	src := o11y.ObserveFeatureFlags(featureflag.Static{"express-shipping": true})
	if !src.Enabled(context.Background(), "express-shipping", featureflag.NoSubject, false) {
		t.Error("the wrapper must still answer when metrics are a no-op")
	}
}
