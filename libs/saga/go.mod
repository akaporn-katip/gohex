module github.com/akaporn-katip/gohex/libs/saga

go 1.26

require (
	github.com/akaporn-katip/gohex/libs/broker v0.0.0
	github.com/akaporn-katip/gohex/libs/cqrs v0.0.0
	github.com/akaporn-katip/gohex/libs/eventstore v0.0.0
	github.com/akaporn-katip/gohex/libs/kernel v0.0.0
	github.com/akaporn-katip/gohex/libs/relay v0.0.0
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace (
	github.com/akaporn-katip/gohex/libs/broker => ../broker
	github.com/akaporn-katip/gohex/libs/cqrs => ../cqrs
	github.com/akaporn-katip/gohex/libs/eventstore => ../eventstore
	github.com/akaporn-katip/gohex/libs/kernel => ../kernel
	github.com/akaporn-katip/gohex/libs/relay => ../relay
)
