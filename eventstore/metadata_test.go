package eventstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
)

func TestAddContextMetadata_mergesTwoExtractors(t *testing.T) {
	// Reset state before the test
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })

	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"trace": "abc123"}
	})
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"flags": "new-pricing"}
	})

	md := eventstore.MetadataFromContext(context.Background())
	if md["trace"] != "abc123" {
		t.Errorf("trace = %q, want abc123", md["trace"])
	}
	if md["flags"] != "new-pricing" {
		t.Errorf("flags = %q, want new-pricing", md["flags"])
	}
	if len(md) != 2 {
		t.Errorf("got %d keys, want 2", len(md))
	}
}

func TestAddContextMetadata_laterExtractorWins(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })

	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"key": "first"}
	})
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"key": "second"}
	})

	md := eventstore.MetadataFromContext(context.Background())
	if md["key"] != "second" {
		t.Errorf("key = %q, want second (later extractor should win)", md["key"])
	}
}

func TestMetadataFromContext_nilWhenNoExtractors(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })
	// SetContextMetadata with nil-returning function means no metadata
	md := eventstore.MetadataFromContext(context.Background())
	if md != nil {
		t.Errorf("got %v, want nil when extractor returns empty", md)
	}
}

func TestMetadataFromContext_nilWhenExtractorsReturnEmpty(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })

	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return nil
	})
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{} // explicitly empty
	})

	md := eventstore.MetadataFromContext(context.Background())
	if md != nil {
		t.Errorf("got %v, want nil when all extractors return empty", md)
	}
}

func TestMetadataFromContext_skipNilExtractorResults(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })

	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return nil // first returns nothing
	})
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"present": "value"}
	})

	md := eventstore.MetadataFromContext(context.Background())
	if md["present"] != "value" {
		t.Errorf("present = %q, want value", md["present"])
	}
	if len(md) != 1 {
		t.Errorf("got %d keys, want 1", len(md))
	}
}

func TestMetadataFromContext_concurrent(t *testing.T) {
	eventstore.SetContextMetadata(func(context.Context) map[string]string { return nil })

	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"base": "value"}
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			md := eventstore.MetadataFromContext(context.Background())
			if md["base"] != "value" {
				t.Errorf("base = %q, want value", md["base"])
			}
		}()
	}
	wg.Wait()
}

func TestSetContextMetadata_replacesExtractors(t *testing.T) {
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"first": "value"}
	})

	eventstore.SetContextMetadata(func(ctx context.Context) map[string]string {
		return map[string]string{"second": "value"}
	})

	md := eventstore.MetadataFromContext(context.Background())
	if _, ok := md["first"]; ok {
		t.Error("first key present after SetContextMetadata; should have been replaced")
	}
	if md["second"] != "value" {
		t.Errorf("second = %q, want value", md["second"])
	}
}
