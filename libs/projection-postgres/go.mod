module github.com/akaporn-katip/gohex/libs/projection-postgres

go 1.26

require (
	github.com/akaporn-katip/gohex/libs/broker v0.0.0
	github.com/akaporn-katip/gohex/libs/projection v0.0.0
	github.com/jackc/pgx/v5 v5.7.6
)

require (
	github.com/akaporn-katip/gohex/libs/eventstore v0.0.0 // indirect
	github.com/akaporn-katip/gohex/libs/kernel v0.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.13.0 // indirect
	golang.org/x/text v0.24.0 // indirect
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace (
	github.com/akaporn-katip/gohex/libs/broker => ../broker
	github.com/akaporn-katip/gohex/libs/eventstore => ../eventstore
	github.com/akaporn-katip/gohex/libs/kernel => ../kernel
	github.com/akaporn-katip/gohex/libs/projection => ../projection
)
