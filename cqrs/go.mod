module github.com/akaporn-katip/gohex/cqrs

go 1.26

require (
	github.com/akaporn-katip/gohex/broker v0.0.0
	github.com/akaporn-katip/gohex/kernel v0.0.0
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace (
	github.com/akaporn-katip/gohex/broker => ../broker
	github.com/akaporn-katip/gohex/kernel => ../kernel
)
