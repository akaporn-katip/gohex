package featureflag

import (
	"context"
	"strings"
	"sync"
)

// FlagsKey is the metadata key holding a comma-separated list of enabled
// flag names. Absent from the list means off. This key is stamped onto
// every event when the pinning extractor is registered via
// eventstore.AddContextMetadata (see [Install]).
const FlagsKey = "gohex.flags"

// Subject is who a flag is evaluated for — a user, a tenant, an order —
// reduced to a key plus optional attributes. The [Source] decides how
// Key and Attrs map to its own targeting rules (allowlists, percentage
// rollouts). Subject is a required argument on [Client.Enabled];
// forgetting to pass it must not silently succeed.
type Subject struct {
	// Key uniquely identifies the subject. Many flag backends use it
	// for percentage-based rollouts; the hash of the key determines
	// which bucket the subject falls into.
	Key string
	// Attrs holds optional targeting attributes (e.g., "region=eu",
	// "tier=premium"). The Source interprets them.
	Attrs map[string]string
}

// NoSubject is a Subject with an empty key and no attributes. Use it
// when a flag genuinely has no subject — a system-wide kill switch —
// rather than when you forgot to pass one (which is a mistake).
var NoSubject = Subject{}

// Source evaluates one boolean flag for a Subject. Adapters (OpenFeature,
// environment variables, static maps) implement this.
type Source interface {
	// Enabled returns whether flag is on for subj, using defaultValue
	// when the flag is undefined or the backend is unreachable.
	Enabled(ctx context.Context, flag string, subj Subject, defaultValue bool) bool
}

// Client is what users call to check flags. Construct one with a [Source]
// and inject it into handlers and command processors that evaluate flags.
// Do not give a Client to downstream constructors (sagas, projections,
// worklist workers); they read pinned decisions via [PinnedIn] instead.
type Client struct {
	src Source
}

// NewClient returns a Client that delegates flag evaluation to src.
func NewClient(src Source) *Client {
	return &Client{src: src}
}

// Enabled returns whether flag is on for subj, using the Source's answer.
// When a recorder is installed on ctx (via [Record]), every flag that
// evaluates to true is recorded so it can be pinned into event metadata.
// defaultValue is passed to the Source and used when the flag is
// undefined or the backend is unreachable.
func (c *Client) Enabled(ctx context.Context, flag string, subj Subject, defaultValue bool) bool {
	enabled := c.src.Enabled(ctx, flag, subj, defaultValue)
	if enabled {
		if r := recorderFromContext(ctx); r != nil {
			r.record(flag)
		}
	}
	return enabled
}

// recorder accumulates the names of flags that evaluated true during a
// single request. It is installed on the context by [Record] and read by
// the pinning extractor.
type recorder struct {
	mu    sync.Mutex
	flags []string
}

func (r *recorder) record(flag string) {
	r.mu.Lock()
	r.flags = append(r.flags, flag)
	r.mu.Unlock()
}

func (r *recorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.flags) == 0 {
		return nil
	}
	out := make([]string, len(r.flags))
	copy(out, r.flags)
	return out
}

type recorderKey struct{}

// Record installs a per-request recorder on ctx and returns the new
// context. Call this at the edge — an HTTP handler, a command handler,
// the entry point of a flow — before any [Client.Enabled] checks. The
// recorder accumulates every flag that evaluated true; the pinning
// extractor (installed via [Install]) stamps them onto appended events.
func Record(ctx context.Context) context.Context {
	return context.WithValue(ctx, recorderKey{}, &recorder{})
}

func recorderFromContext(ctx context.Context) *recorder {
	r, _ := ctx.Value(recorderKey{}).(*recorder)
	return r
}

// Install registers the pinning extractor with the event store. Call
// this at startup, after o11y.Init and before the first [Record].
// The extractor reads the recorder from the context and stamps its
// enabled flags under [FlagsKey].
//
// This function is defined in install.go to avoid importing eventstore
// into the featureflag package, which would create a cycle (eventstore
// must not import featureflag, and featureflag must not import o11y).
// See install.go for the implementation.

// PinnedIn returns whether flag was enabled when the edge evaluated it,
// reading from the metadata map that traveled with the event or message.
// Use this in sagas, projections, and workers rather than re-evaluating
// the flag, so the decision stays stable across the entire causal chain.
func PinnedIn(md map[string]string, flag string) bool {
	list, ok := md[FlagsKey]
	if !ok || list == "" {
		return false
	}
	// The list is comma-separated flag names. We could split and search,
	// but a substring check with delimiters is simpler and faster for
	// small lists (which is what we expect: a handful of active flags).
	// Edge case: "new-pricing" must not match "super-new-pricing-v2".
	if list == flag {
		return true
	}
	if strings.HasPrefix(list, flag+",") {
		return true
	}
	if strings.HasSuffix(list, ","+flag) {
		return true
	}
	if strings.Contains(list, ","+flag+",") {
		return true
	}
	return false
}
