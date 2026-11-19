package o11y_test

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/o11y"
)

// A service that cannot build its resource cannot start. Init used to
// merge the SDK's default resource with one labelled semconv v1.26.0;
// under SDK 1.47 the default speaks schema 1.43, Merge refused the pair,
// and every service died on boot with "conflicting Schema URL". No test
// saw it: the only Init the suite ran set WithoutExporter, which returns
// before the resource is built.
//
// So this one takes the exporter path. The endpoint points at a port
// nothing listens on — OTLP/HTTP connects lazily, so Init still has to
// succeed, and what it proves is that the resource was built at all.
func TestInitBuildsResourceOnTheExporterPath(t *testing.T) {
	ctx := context.Background()

	shutdown, err := o11y.Init(ctx, o11y.Config{
		ServiceName:  "resource-test",
		OTLPEndpoint: "127.0.0.1:1",
	})
	if err != nil {
		t.Fatalf("Init with an exporter: %v", err)
	}
	t.Cleanup(func() { _ = shutdown(ctx) })
}
