package featureflag

import (
	"context"
	"strings"

	"github.com/akaporn-katip/gohex/eventstore"
)

// Install registers the pinning extractor with the event store. Call
// this at startup, after o11y.Init and before the first [Record]. The
// extractor reads the recorder from the context and stamps its enabled
// flags under [FlagsKey] as a comma-separated list.
func Install() {
	eventstore.AddContextMetadata(func(ctx context.Context) map[string]string {
		r := recorderFromContext(ctx)
		if r == nil {
			return nil
		}
		flags := r.snapshot()
		if len(flags) == 0 {
			return nil
		}
		return map[string]string{
			FlagsKey: strings.Join(flags, ","),
		}
	})
}
