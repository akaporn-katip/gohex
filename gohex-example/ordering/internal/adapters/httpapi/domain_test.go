package httpapi

import "testing"

// TestDomainOf: the edge sends the flag backend a class of account, never
// a person — everything this function drops is deliberate.
func TestDomainOf(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"e2e+1795447041-iqc7tbkr@horstory.test", "horstory.test"},
		{"Alice@HORSTORY.Test", "horstory.test"},
		{"bob@example.com", "example.com"},
		{"weird@sub@horstory.test", "horstory.test"},
		{"", ""},
		{"no-at-sign", ""},
		{"trailing@", ""},
	} {
		if got := domainOf(tc.in); got != tc.want {
			t.Errorf("domainOf(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
