module github.com/akaporn-katip/gohex/services/ordering

go 1.26

require (
	github.com/akaporn-katip/gohex/libs/broker v0.0.0
	github.com/akaporn-katip/gohex/libs/broker-kafka v0.0.0
	github.com/akaporn-katip/gohex/libs/cqrs v0.0.0
	github.com/akaporn-katip/gohex/libs/eventstore v0.0.0
	github.com/akaporn-katip/gohex/libs/eventstore-postgres v0.0.0
	github.com/akaporn-katip/gohex/libs/kernel v0.0.0
	github.com/akaporn-katip/gohex/libs/o11y v0.0.0
	github.com/akaporn-katip/gohex/libs/projection v0.0.0
	github.com/akaporn-katip/gohex/libs/projection-postgres v0.0.0
	github.com/akaporn-katip/gohex/libs/relay v0.0.0
	github.com/akaporn-katip/gohex/libs/saga v0.0.0
	github.com/akaporn-katip/gohex/services/contracts v0.0.0
	github.com/jackc/pgx/v5 v5.7.6
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.60.0
	golang.org/x/sync v0.13.0
)

require (
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.26.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.17.8 // indirect
	github.com/pierrec/lz4/v4 v4.1.21 // indirect
	github.com/twmb/franz-go v1.18.0 // indirect
	github.com/twmb/franz-go/pkg/kmsg v1.9.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/otel v1.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.35.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.35.0 // indirect
	go.opentelemetry.io/otel/metric v1.35.0 // indirect
	go.opentelemetry.io/otel/sdk v1.35.0 // indirect
	go.opentelemetry.io/otel/trace v1.35.0 // indirect
	go.opentelemetry.io/proto/otlp v1.5.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.35.0 // indirect
	golang.org/x/sys v0.32.0 // indirect
	golang.org/x/text v0.24.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250218202821-56aae31c358a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250218202821-56aae31c358a // indirect
	google.golang.org/grpc v1.71.0 // indirect
	google.golang.org/protobuf v1.36.5 // indirect
)

// Pre-release: modules are developed in the gohex workspace and not yet
// tagged. Drop the replace directives at the first tagged release.
replace (
	github.com/akaporn-katip/gohex/libs/broker => ../../libs/broker
	github.com/akaporn-katip/gohex/libs/broker-kafka => ../../libs/broker-kafka
	github.com/akaporn-katip/gohex/libs/cqrs => ../../libs/cqrs
	github.com/akaporn-katip/gohex/libs/eventstore => ../../libs/eventstore
	github.com/akaporn-katip/gohex/libs/eventstore-postgres => ../../libs/eventstore-postgres
	github.com/akaporn-katip/gohex/libs/kernel => ../../libs/kernel
	github.com/akaporn-katip/gohex/libs/o11y => ../../libs/o11y
	github.com/akaporn-katip/gohex/libs/projection => ../../libs/projection
	github.com/akaporn-katip/gohex/libs/projection-postgres => ../../libs/projection-postgres
	github.com/akaporn-katip/gohex/libs/relay => ../../libs/relay
	github.com/akaporn-katip/gohex/libs/saga => ../../libs/saga
	github.com/akaporn-katip/gohex/services/contracts => ../contracts
)
