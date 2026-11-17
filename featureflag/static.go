package featureflag

import "context"

// Static is a [Source] that answers from a fixed map, with no backend.
// Use it for tests or services that evaluate flags but delegate the
// decision to the edge service and read pinned values via [PinnedIn].
type Static map[string]bool

// Enabled returns the value from the map, or defaultValue when the flag
// is not in the map. The Subject is ignored; Static has no targeting.
func (s Static) Enabled(ctx context.Context, flag string, subj Subject, defaultValue bool) bool {
	if val, ok := s[flag]; ok {
		return val
	}
	return defaultValue
}
