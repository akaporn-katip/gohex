package o11y

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/akaporn-katip/gohex/featureflag"
)

// ObserveFeatureFlags wraps a flag source so every evaluation is
// counted: one series per flag and outcome, under
// "gohex.featureflag.evaluations" with gohex.featureflag.name and
// gohex.featureflag.enabled.
//
// What the counter is for is deletion. A release flag is temporary by
// definition, and the two facts that say a flag is ready to go —
// nobody asks it any more, or everybody gets the same answer — are
// invisible in the code and obvious in this metric: a series that has
// stopped moving is dead configuration, and a flag whose enabled=false
// series is flat at zero has finished rolling out.
//
// The point carries the flag's name and the answer, never the
// [featureflag.Subject]: the subject key is a customer or tenant
// identifier, which would both explode cardinality and put customer
// data in a metric that outlives every log it appears in — the same
// reason [RunnerRestartHook] carries a runner name and no error.
//
// Wrapping the Source rather than the Client is deliberate: the Client
// owns the pinning behaviour (recording what evaluated true), so
// decorating it would risk a wrapper that answers without recording.
// Wrap at the edge of the hexagon instead:
//
//	src := o11y.ObserveFeatureFlags(openfeature.New(client))
//	flags := featureflag.NewClient(src)
//
// With no MeterProvider installed the global meter is a no-op, so this
// costs nothing and stays wired on a collector-less laptop run.
func ObserveFeatureFlags(next featureflag.Source) featureflag.Source {
	counter, err := otel.Meter(scope).Int64Counter("gohex.featureflag.evaluations",
		metric.WithUnit("{evaluation}"),
		metric.WithDescription("Feature flag evaluations, by flag and answer — the signal that a flag is finished and can be deleted."),
	)
	if err != nil {
		// A counter with a constant, valid name only fails to build on a
		// broken meter. Flag evaluation is request-path work: answering
		// uncounted beats failing to answer.
		return next
	}
	return &observedSource{next: next, counter: counter}
}

type observedSource struct {
	next    featureflag.Source
	counter metric.Int64Counter
}

func (s *observedSource) Enabled(ctx context.Context, flag string, subj featureflag.Subject, defaultValue bool) bool {
	enabled := s.next.Enabled(ctx, flag, subj, defaultValue)
	s.counter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("gohex.featureflag.name", flag),
		attribute.Bool("gohex.featureflag.enabled", enabled),
	))
	return enabled
}
