package openfeature_test

import (
	"context"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"

	"github.com/akaporn-katip/gohex/featureflag"
	ffof "github.com/akaporn-katip/gohex/featureflag-openfeature"
)

// inMemoryProvider is a minimal OpenFeature provider for testing. It
// evaluates flags from a static map with no targeting logic.
type inMemoryProvider struct {
	flags map[string]bool
}

func (p *inMemoryProvider) Metadata() openfeature.Metadata {
	return openfeature.Metadata{Name: "in-memory"}
}

func (p *inMemoryProvider) BooleanEvaluation(
	ctx context.Context,
	flag string,
	defaultValue bool,
	evalCtx openfeature.FlattenedContext,
) openfeature.BoolResolutionDetail {
	if val, ok := p.flags[flag]; ok {
		return openfeature.BoolResolutionDetail{
			Value: val,
			ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
				Reason: openfeature.TargetingMatchReason,
			},
		}
	}
	return openfeature.BoolResolutionDetail{
		Value: defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			Reason: openfeature.DefaultReason,
		},
	}
}

func (p *inMemoryProvider) Hooks() []openfeature.Hook {
	return nil
}

func (p *inMemoryProvider) StringEvaluation(
	ctx context.Context,
	flag string,
	defaultValue string,
	evalCtx openfeature.FlattenedContext,
) openfeature.StringResolutionDetail {
	return openfeature.StringResolutionDetail{
		Value: defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			Reason: openfeature.DefaultReason,
		},
	}
}

func (p *inMemoryProvider) FloatEvaluation(
	ctx context.Context,
	flag string,
	defaultValue float64,
	evalCtx openfeature.FlattenedContext,
) openfeature.FloatResolutionDetail {
	return openfeature.FloatResolutionDetail{
		Value: defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			Reason: openfeature.DefaultReason,
		},
	}
}

func (p *inMemoryProvider) IntEvaluation(
	ctx context.Context,
	flag string,
	defaultValue int64,
	evalCtx openfeature.FlattenedContext,
) openfeature.IntResolutionDetail {
	return openfeature.IntResolutionDetail{
		Value: defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			Reason: openfeature.DefaultReason,
		},
	}
}

func (p *inMemoryProvider) ObjectEvaluation(
	ctx context.Context,
	flag string,
	defaultValue interface{},
	evalCtx openfeature.FlattenedContext,
) openfeature.InterfaceResolutionDetail {
	return openfeature.InterfaceResolutionDetail{
		Value: defaultValue,
		ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			Reason: openfeature.DefaultReason,
		},
	}
}

func TestSource_Enabled(t *testing.T) {
	provider := &inMemoryProvider{
		flags: map[string]bool{
			"feature-on":  true,
			"feature-off": false,
		},
	}
	if err := openfeature.SetProviderAndWait(provider); err != nil {
		t.Fatalf("SetProviderAndWait: %v", err)
	}
	client := openfeature.NewClient("test")

	src := ffof.New(client)
	subj := featureflag.Subject{
		Key:   "user-123",
		Attrs: map[string]string{"region": "us-west"},
	}
	ctx := context.Background()

	// Flag explicitly enabled
	if !src.Enabled(ctx, "feature-on", subj, false) {
		t.Error("feature-on should be true")
	}

	// Flag explicitly disabled
	if src.Enabled(ctx, "feature-off", subj, true) {
		t.Error("feature-off should be false")
	}

	// Undefined flag uses default
	if !src.Enabled(ctx, "undefined", subj, true) {
		t.Error("undefined should use default true")
	}
	if src.Enabled(ctx, "undefined", subj, false) {
		t.Error("undefined should use default false")
	}
}

func TestSource_SubjectMapping(t *testing.T) {
	provider := &inMemoryProvider{
		flags: map[string]bool{"any-flag": true},
	}
	if err := openfeature.SetProviderAndWait(provider); err != nil {
		t.Fatalf("SetProviderAndWait: %v", err)
	}
	client := openfeature.NewClient("test")

	src := ffof.New(client)

	// NoSubject still works (empty key)
	if !src.Enabled(context.Background(), "any-flag", featureflag.NoSubject, false) {
		t.Error("flag should work with NoSubject")
	}

	// Subject with attributes
	subj := featureflag.Subject{
		Key:   "tenant-456",
		Attrs: map[string]string{"tier": "premium", "region": "eu"},
	}
	if !src.Enabled(context.Background(), "any-flag", subj, false) {
		t.Error("flag should work with full Subject")
	}
}
