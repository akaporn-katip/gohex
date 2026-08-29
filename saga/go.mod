module github.com/akaporn-katip/gohex/saga

go 1.26

require (
	github.com/akaporn-katip/gohex/broker v0.0.0
	github.com/akaporn-katip/gohex/cqrs v0.0.0
	github.com/akaporn-katip/gohex/eventstore v0.0.0
	github.com/akaporn-katip/gohex/kernel v0.0.0
	github.com/akaporn-katip/gohex/relay v0.0.0
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace (
	github.com/akaporn-katip/gohex/broker => ../broker
	github.com/akaporn-katip/gohex/cqrs => ../cqrs
	github.com/akaporn-katip/gohex/eventstore => ../eventstore
	github.com/akaporn-katip/gohex/kernel => ../kernel
	github.com/akaporn-katip/gohex/relay => ../relay
)
