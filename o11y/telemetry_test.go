package o11y_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otellog "go.opentelemetry.io/otel/log"
	logglobal "go.opentelemetry.io/otel/log/global"
	lognoop "go.opentelemetry.io/otel/log/noop"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/o11y"
)

// --- logs: the fan-out -----------------------------------------------

// TestSlogHandlerFansOutToStdoutAndOTLP is the heart of the log half:
// ONE slog call must still produce the stdout JSON line an operator
// greps AND an OTLP record a backend can join to the trace.
func TestSlogHandlerFansOutToStdoutAndOTLP(t *testing.T) {
	setup(t)
	records := &recordExporter{}
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(records)))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	var buf strings.Builder
	logger := slog.New(o11y.NewSlogHandler(&buf, "test", provider))
	ctx, span := rootSpan(t)
	defer span.End()
	logger.InfoContext(ctx, "lease started", "tenant", "acme")

	// stdout side: unchanged JSON, trace correlation as attributes.
	var line map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &line); err != nil {
		t.Fatalf("stdout line is not JSON: %v (%q)", err, buf.String())
	}
	if line["msg"] != "lease started" || line["tenant"] != "acme" || line["level"] != "INFO" {
		t.Errorf("stdout line lost fields: %v", line)
	}
	if line["trace_id"] != span.SpanContext().TraceID().String() {
		t.Errorf("stdout trace_id = %v, want %s", line["trace_id"], span.SpanContext().TraceID())
	}
	if line["span_id"] != span.SpanContext().SpanID().String() {
		t.Errorf("stdout span_id = %v, want %s", line["span_id"], span.SpanContext().SpanID())
	}

	// OTLP side: same record, correlation as first-class fields.
	got := records.all()
	if len(got) != 1 {
		t.Fatalf("emitted %d OTLP records, want 1", len(got))
	}
	rec := got[0]
	if rec.Body().AsString() != "lease started" {
		t.Errorf("OTLP body = %q, want %q", rec.Body().AsString(), "lease started")
	}
	if rec.TraceID() != span.SpanContext().TraceID() {
		t.Errorf("OTLP trace id = %s, want %s", rec.TraceID(), span.SpanContext().TraceID())
	}
	if rec.SpanID() != span.SpanContext().SpanID() {
		t.Errorf("OTLP span id = %s, want %s", rec.SpanID(), span.SpanContext().SpanID())
	}
	attrs := map[string]string{}
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[string(kv.Key)] = kv.Value.String()
		return true
	})
	if attrs["tenant"] != "acme" {
		t.Errorf("OTLP attributes = %v, want tenant=acme", attrs)
	}
}

// TestSlogHandlerWithoutProviderIsStdoutOnly is the collector-less
// path: no LoggerProvider, same line on stdout, nothing else to go
// wrong.
func TestSlogHandlerWithoutProviderIsStdoutOnly(t *testing.T) {
	setup(t)
	var buf strings.Builder
	logger := slog.New(o11y.NewSlogHandler(&buf, "test", nil))
	ctx, span := rootSpan(t)
	defer span.End()
	logger.InfoContext(ctx, "quiet")

	var line map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &line); err != nil {
		t.Fatalf("stdout line is not JSON: %v (%q)", err, buf.String())
	}
	if line["msg"] != "quiet" || line["trace_id"] != span.SpanContext().TraceID().String() {
		t.Errorf("stdout line = %v, want the message and its trace", line)
	}
}

// TestFanoutDeliversToEverySinkDespiteFailure: a sink that errors must
// not swallow another sink's record — losing the OTLP collector must
// not cost us the stdout line, nor the reverse.
func TestFanoutDeliversToEverySinkDespiteFailure(t *testing.T) {
	var a, b strings.Builder
	broken := &failingHandler{}
	h := o11y.Fanout(
		slog.NewJSONHandler(&a, nil),
		broken,
		slog.NewJSONHandler(&b, nil),
	)
	logger := slog.New(h)
	logger.Info("both")

	if a.Len() == 0 || b.Len() == 0 {
		t.Errorf("a sink missed the record: a=%q b=%q", a.String(), b.String())
	}
	if broken.calls != 1 {
		t.Errorf("broken sink called %d times, want 1", broken.calls)
	}
	if err := h.Handle(context.Background(), slog.Record{}); err == nil {
		t.Error("Handle swallowed the failing sink's error")
	}
}

// TestFanoutPropagatesAttrsAndGroups: slog builds handlers up with
// With/WithGroup before any record exists; every sink must see them.
func TestFanoutPropagatesAttrsAndGroups(t *testing.T) {
	var a, b strings.Builder
	logger := slog.New(o11y.Fanout(slog.NewJSONHandler(&a, nil), slog.NewJSONHandler(&b, nil)))
	logger.With("service", "billing").WithGroup("lease").Info("started", "id", "L1")

	for name, got := range map[string]string{"a": a.String(), "b": b.String()} {
		var line map[string]any
		if err := json.Unmarshal([]byte(got), &line); err != nil {
			t.Fatalf("sink %s: %v (%q)", name, err, got)
		}
		if line["service"] != "billing" {
			t.Errorf("sink %s lost the With attr: %v", name, line)
		}
		group, _ := line["lease"].(map[string]any)
		if group["id"] != "L1" {
			t.Errorf("sink %s lost the group: %v", name, line)
		}
	}
}

// --- metrics: the backlog gauges -------------------------------------

func TestBacklogGaugesSampleHeadMinusCheckpoint(t *testing.T) {
	reader := meterProvider(t)
	checkpoints := eventstore.NewMemoryCheckpointStore()
	ctx := context.Background()
	if err := checkpoints.Set(ctx, "ordering.relay", 90); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.Set(ctx, "billing_views.store", 40); err != nil {
		t.Fatal(err)
	}
	if err := checkpoints.Set(ctx, "billing_views.inbox", 7); err != nil {
		t.Fatal(err)
	}
	head := func(at int64) o11y.Position {
		return func(context.Context) (int64, error) { return at, nil }
	}

	if _, err := o11y.WatchRelayLag("ordering.relay", head(100),
		o11y.CheckpointPosition(checkpoints, "ordering.relay")); err != nil {
		t.Fatal(err)
	}
	if _, err := o11y.WatchProjectionLag("billing_views", "store", head(100),
		o11y.CheckpointPosition(checkpoints, "billing_views.store")); err != nil {
		t.Fatal(err)
	}
	if _, err := o11y.WatchInboxDepth("billing_views", head(10),
		o11y.CheckpointPosition(checkpoints, "billing_views.inbox")); err != nil {
		t.Fatal(err)
	}

	got := collectGauges(t, reader)
	want := map[string]int64{
		"gohex.relay.lag":      10,
		"gohex.projection.lag": 60,
		"gohex.inbox.depth":    3,
	}
	for name, value := range want {
		point, ok := got[name]
		if !ok {
			t.Errorf("%s was never collected (have %v)", name, keys(got))
			continue
		}
		if point.value != value {
			t.Errorf("%s = %d, want %d", name, point.value, value)
		}
	}
	// Identity lives in the attributes, so one dashboard row per worker.
	relayAttrs := got["gohex.relay.lag"].attrs
	if v, ok := relayAttrs.Value("gohex.relay.name"); !ok || v.AsString() != "ordering.relay" {
		t.Errorf("relay lag attributes = %v", relayAttrs.Encoded(attribute.DefaultEncoder()))
	}
	projectionAttrs := got["gohex.projection.lag"].attrs
	if source, ok := projectionAttrs.Value("gohex.projection.source"); !ok || source.AsString() != "store" {
		t.Errorf("projection lag must distinguish store from inbox, got %v",
			projectionAttrs.Encoded(attribute.DefaultEncoder()))
	}
	if unit := got["gohex.inbox.depth"].unit; unit != "{message}" {
		t.Errorf("inbox depth unit = %q, want {message}", unit)
	}
}

// TestBacklogGaugeUnregisters: a stopped worker stops reporting, rather
// than freezing its last lag forever on the dashboard.
func TestBacklogGaugeUnregisters(t *testing.T) {
	reader := meterProvider(t)
	head := func(context.Context) (int64, error) { return 5, nil }
	position := func(context.Context) (int64, error) { return 1, nil }
	unwatch, err := o11y.WatchRelayLag("ordering.relay", head, position)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := collectGauges(t, reader)["gohex.relay.lag"]; !ok {
		t.Fatal("gauge never reported while watched")
	}
	if err := unwatch(); err != nil {
		t.Fatal(err)
	}
	if _, ok := collectGauges(t, reader)["gohex.relay.lag"]; ok {
		t.Error("gauge still reporting after Unwatch")
	}
}

// TestBacklogGaugeFloorsTornRead: head and checkpoint are two separate
// reads, so the worker can advance in between; a negative backlog is
// nonsense, not news.
func TestBacklogGaugeFloorsTornRead(t *testing.T) {
	reader := meterProvider(t)
	head := func(context.Context) (int64, error) { return 40, nil }
	position := func(context.Context) (int64, error) { return 41, nil }
	if _, err := o11y.WatchProjectionLag("billing_views", "inbox", head, position); err != nil {
		t.Fatal(err)
	}
	if got := collectGauges(t, reader)["gohex.projection.lag"].value; got != 0 {
		t.Errorf("torn read reported %d, want 0", got)
	}
}

// TestBacklogGaugeReportsQueryFailure: a head query that fails must
// surface as a collection error, not as a lag of zero — "no backlog"
// and "cannot tell" are opposite operational answers.
func TestBacklogGaugeReportsQueryFailure(t *testing.T) {
	reader := meterProvider(t)
	boom := errors.New("connection refused")
	head := func(context.Context) (int64, error) { return 0, boom }
	position := func(context.Context) (int64, error) { return 0, nil }
	if _, err := o11y.WatchRelayLag("ordering.relay", head, position); err != nil {
		t.Fatal(err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); !errors.Is(err, boom) {
		t.Errorf("Collect error = %v, want it to wrap %v", err, boom)
	}
}

func TestWatchBacklogRequiresBothPositions(t *testing.T) {
	meterProvider(t)
	position := func(context.Context) (int64, error) { return 0, nil }
	if _, err := o11y.WatchRelayLag("ordering.relay", nil, position); err == nil {
		t.Error("a watch with no head position must fail loudly, not report zero forever")
	}
	if _, err := o11y.WatchInboxDepth("billing_views", position, nil); err == nil {
		t.Error("a watch with no checkpoint position must fail loudly")
	}
}

// TestWatchBacklogWithoutMeterProviderIsSilent: WithoutExporter (and a
// collector-less laptop run) must leave the instruments harmless.
func TestWatchBacklogWithoutMeterProviderIsSilent(t *testing.T) {
	if _, err := o11y.Init(context.Background(), o11y.Config{ServiceName: "test", WithoutExporter: true}); err != nil {
		t.Fatal(err)
	}
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
	t.Cleanup(func() { otel.SetMeterProvider(metricnoop.NewMeterProvider()) })

	called := false
	head := func(context.Context) (int64, error) { called = true; return 0, errors.New("never asked") }
	unwatch, err := o11y.WatchRelayLag("ordering.relay", head, head)
	if err != nil {
		t.Fatalf("watching a backlog with no MeterProvider failed: %v", err)
	}
	if called {
		t.Error("the no-op meter sampled the backlog")
	}
	if err := unwatch(); err != nil {
		t.Fatal(err)
	}
}

// --- Init: all three signals, one endpoint ---------------------------

// TestInitExportsAllThreeSignals runs Init against a stand-in collector
// and checks the whole contract in one go: traces still export, logs
// reach OTLP *and* stdout, metrics export at all, and the shutdown that
// Init returns flushes every one of them (nothing here waits for a
// periodic interval — the exports arrive because shutdown forced them).
func TestInitExportsAllThreeSignals(t *testing.T) {
	collector := &fakeCollector{}
	server := httptest.NewServer(collector)
	defer server.Close()

	restore := captureStdout(t)
	shutdown, err := o11y.Init(context.Background(), o11y.Config{
		ServiceName:  "test",
		OTLPEndpoint: strings.TrimPrefix(server.URL, "http://"),
	})
	if err != nil {
		restore()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		logglobal.SetLoggerProvider(lognoop.NewLoggerProvider())
	})

	ctx, span := otel.Tracer("test").Start(context.Background(), "http.request")
	slog.InfoContext(ctx, "lease started")
	span.End()

	head := func(context.Context) (int64, error) { return 7, nil }
	position := func(context.Context) (int64, error) { return 2, nil }
	if _, err := o11y.WatchRelayLag("ordering.relay", head, position); err != nil {
		restore()
		t.Fatal(err)
	}

	if err := shutdown(context.Background()); err != nil {
		restore()
		t.Fatalf("shutdown: %v", err)
	}
	out := restore()

	for _, path := range []string{"/v1/traces", "/v1/logs", "/v1/metrics"} {
		if collector.count(path) == 0 {
			t.Errorf("collector never received %s (got %v)", path, collector.paths())
		}
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &line); err != nil {
		t.Fatalf("stdout no longer carries the JSON line: %v (%q)", err, out)
	}
	if line["msg"] != "lease started" || line["trace_id"] == "" {
		t.Errorf("stdout line = %v, want the message with its trace_id", line)
	}
}

func TestInitWithoutExporterShutdownSucceeds(t *testing.T) {
	shutdown, err := o11y.Init(context.Background(), o11y.Config{ServiceName: "test", WithoutExporter: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("collector-less shutdown = %v, want nil", err)
	}
}

// --- helpers ---------------------------------------------------------

// meterProvider installs a manually-read MeterProvider and returns the
// reader, so a test can collect on demand.
func meterProvider(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(provider)
	t.Cleanup(func() {
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		_ = provider.Shutdown(context.Background())
	})
	return reader
}

type gaugePoint struct {
	value int64
	unit  string
	attrs attribute.Set
}

func collectGauges(t *testing.T, reader *sdkmetric.ManualReader) map[string]gaugePoint {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	out := map[string]gaugePoint{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				continue
			}
			for _, dp := range gauge.DataPoints {
				out[m.Name] = gaugePoint{value: dp.Value, unit: m.Unit, attrs: dp.Attributes}
			}
		}
	}
	return out
}

func keys(m map[string]gaugePoint) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// recordExporter keeps every exported log record for inspection.
type recordExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *recordExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, rec := range records {
		e.records = append(e.records, rec.Clone())
	}
	return nil
}

func (e *recordExporter) Shutdown(context.Context) error   { return nil }
func (e *recordExporter) ForceFlush(context.Context) error { return nil }

func (e *recordExporter) all() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

type failingHandler struct{ calls int }

func (h *failingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *failingHandler) Handle(context.Context, slog.Record) error {
	h.calls++
	return errors.New("sink is down")
}
func (h *failingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *failingHandler) WithGroup(string) slog.Handler      { return h }

// fakeCollector stands in for the in-cluster OTel collector, counting
// the signal endpoints it is asked for.
type fakeCollector struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *fakeCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	c.mu.Lock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[r.URL.Path]++
	c.mu.Unlock()
	w.Header().Set("Content-Type", "application/x-protobuf")
	w.WriteHeader(http.StatusOK)
}

func (c *fakeCollector) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[path]
}

func (c *fakeCollector) paths() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for k, v := range c.counts {
		out[k] = v
	}
	return out
}

// captureStdout swaps os.Stdout for a pipe, since Init logs to the real
// one. The returned restore puts os.Stdout back and yields everything
// written meanwhile; calling it twice is safe and repeats the output.
func captureStdout(t *testing.T) (restore func() string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()

	var once sync.Once
	var out string
	restore = func() string {
		once.Do(func() {
			os.Stdout = saved
			_ = w.Close()
			out = <-done
			_ = r.Close()
		})
		return out
	}
	t.Cleanup(func() { restore() })
	return restore
}
