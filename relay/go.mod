// Deprecated: the gohex modules were unified into a single module. Use
// github.com/akaporn-katip/gohex v0.7.0 or later, where this package keeps
// the exact same import path.
module github.com/akaporn-katip/gohex/relay

go 1.26

require (
	github.com/akaporn-katip/gohex/broker v0.1.0
	github.com/akaporn-katip/gohex/eventstore v0.2.0
	github.com/akaporn-katip/gohex/kernel v0.1.0
)

// Every version of this module is retracted: its packages now live in
// github.com/akaporn-katip/gohex at the same import paths.
retract [v0.1.0, v0.2.1] // Unified into github.com/akaporn-katip/gohex v0.7.0.
