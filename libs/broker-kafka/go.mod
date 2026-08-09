module github.com/akaporn-katip/gohex/libs/broker-kafka

go 1.26

require (
	github.com/akaporn-katip/gohex/libs/broker v0.0.0
	github.com/twmb/franz-go v1.18.0
)

require (
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace github.com/akaporn-katip/gohex/libs/broker => ../broker
