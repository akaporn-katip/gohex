package featureflag_test

import (
	"context"
	"fmt"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/featureflag"
)

// Example shows the whole life of a flag decision: evaluated once at the
// edge, stamped onto the events that edge produces, and read back by a
// downstream hop that must not ask the question again (ADR-0021).
func Example() {
	// Startup, once: the pinning extractor stamps whatever the edge
	// recorded onto every event the repository appends.
	featureflag.Install()
	client := featureflag.NewClient(featureflag.Static{
		"new-pricing": true,
		"beta-ship":   false,
	})

	// The edge — an HTTP handler here — opens a recorder and evaluates
	// for a subject. This is the only place in the system that asks.
	ctx := featureflag.Record(context.Background())
	customer := featureflag.Subject{
		Key:   "customer-42",
		Attrs: map[string]string{"tier": "premium"},
	}
	if client.Enabled(ctx, "new-pricing", customer, false) {
		_ = "price the order the new way"
	}
	client.Enabled(ctx, "beta-ship", customer, false)

	// eventstore.Repository.Save stamps this map onto every appended
	// event; the relay copies it onto the envelope, the saga runner onto
	// the commands it issues. Only enabled flags are listed.
	metadata := eventstore.MetadataFromContext(ctx)
	fmt.Println(metadata[featureflag.FlagsKey])

	// Three days later, in a saga or a projection: read the decision,
	// never re-evaluate it. A flag missing from the list is off.
	fmt.Println(featureflag.PinnedIn(metadata, "new-pricing"))
	fmt.Println(featureflag.PinnedIn(metadata, "beta-ship"))

	// Output:
	// new-pricing
	// true
	// false
}

// ExampleNoSubject shows an operational kill switch: it applies to the
// whole service, so there is no subject to target. Saying so explicitly
// keeps it distinguishable from forgetting to pass one.
func ExampleNoSubject() {
	client := featureflag.NewClient(featureflag.Static{"pause-shipping": true})

	if client.Enabled(context.Background(), "pause-shipping", featureflag.NoSubject, false) {
		fmt.Println("shipping paused")
	}

	// Output:
	// shipping paused
}
