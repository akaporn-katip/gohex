module github.com/akaporn-katip/gohex/libs/eventstore

go 1.26

require github.com/akaporn-katip/gohex/libs/kernel v0.0.0

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace github.com/akaporn-katip/gohex/libs/kernel => ../kernel
