package openfeature

import (
	"context"

	"github.com/open-feature/go-sdk/openfeature"

	"github.com/akaporn-katip/gohex/featureflag"
)

// Source implements featureflag.Source by delegating to an OpenFeature
// client. Subject.Key becomes the targeting key; Subject.Attrs become
// evaluation context attributes.
type Source struct {
	client *openfeature.Client
}

var _ featureflag.Source = (*Source)(nil)

// New returns a Source that evaluates flags using the given OpenFeature
// client. The caller is responsible for setting the provider on the
// OpenFeature API before constructing the client.
func New(client *openfeature.Client) *Source {
	return &Source{client: client}
}

// Enabled evaluates flag for subj using the OpenFeature client. The
// Subject.Key maps to the targeting key; Subject.Attrs map to evaluation
// context attributes. Returns defaultValue when the flag is undefined or
// evaluation fails.
func (s *Source) Enabled(ctx context.Context, flag string, subj featureflag.Subject, defaultValue bool) bool {
	// Convert string map to any map for OpenFeature
	attrs := make(map[string]any, len(subj.Attrs))
	for k, v := range subj.Attrs {
		attrs[k] = v
	}
	evalCtx := openfeature.NewEvaluationContext(subj.Key, attrs)
	result, _ := s.client.BooleanValue(ctx, flag, defaultValue, evalCtx)
	return result
}
