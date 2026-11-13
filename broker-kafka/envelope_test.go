package kafkabroker

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/akaporn-katip/gohex/broker"
)

// The envelope tests need no Kafka: toRecord and fromRecord are pure.

func TestEnvelopeRoundTrip(t *testing.T) {
	want := broker.Message{
		ID: "payment/9#1", Key: "42", Type: "billing.payment_captured", Version: 3,
		OccurredAt: time.Now().UTC().Truncate(time.Nanosecond),
		Payload:    []byte(`{"order_id":"42"}`),
		Metadata:   map[string]string{"traceparent": "00-abc-def-01"},
	}
	rec, err := toRecord("billing.events", want)
	if err != nil {
		t.Fatalf("toRecord: %v", err)
	}
	got, err := fromRecord(rec)
	if err != nil {
		t.Fatalf("fromRecord: %v", err)
	}
	if got.ID != want.ID || got.Key != want.Key || got.Type != want.Type || got.Version != want.Version {
		t.Errorf("envelope lost data: %+v", got)
	}
	if !got.OccurredAt.Equal(want.OccurredAt) {
		t.Errorf("OccurredAt = %v, want %v", got.OccurredAt, want.OccurredAt)
	}
	if got.Metadata["traceparent"] != want.Metadata["traceparent"] {
		t.Errorf("Metadata = %v", got.Metadata)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("Payload = %s", got.Payload)
	}
}

// A hand-written record with no gohex-* headers is not malformed: it is
// structurally incomplete, and the consumer edge is where that is
// decided and reported. fromRecord must not guess.
func TestFromRecordKeepsAnAbsentHeaderZero(t *testing.T) {
	msg, err := fromRecord(&kgo.Record{Value: []byte(`{"hello":"world"}`)})
	if err != nil {
		t.Fatalf("fromRecord: %v", err)
	}
	if msg.ID != "" || msg.Type != "" || msg.Version != 0 || !msg.OccurredAt.IsZero() {
		t.Errorf("fabricated envelope fields: %+v", msg)
	}
}

// A header that IS there but cannot be parsed must surface, never
// become a plausible-looking Message that matches no handler and is
// skipped in silence.
func TestFromRecordRejectsMalformedHeaders(t *testing.T) {
	cases := []struct {
		name   string
		header kgo.RecordHeader
	}{
		{"a version that is not a number", kgo.RecordHeader{Key: headerVersion, Value: []byte("v1")}},
		{"an empty version", kgo.RecordHeader{Key: headerVersion, Value: []byte("")}},
		{"an occurred-at that is not RFC3339", kgo.RecordHeader{Key: headerOccurredAt, Value: []byte("yesterday")}},
		{"metadata that is not JSON", kgo.RecordHeader{Key: headerMetadata, Value: []byte("trace=abc")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &kgo.Record{
				Partition: 2,
				Offset:    71,
				Value:     []byte(`{}`),
				Headers: []kgo.RecordHeader{
					{Key: headerID, Value: []byte("payment/9#1")},
					{Key: headerType, Value: []byte("billing.payment_captured")},
					tc.header,
				},
			}
			msg, err := fromRecord(rec)
			if err == nil {
				t.Fatalf("fromRecord = %+v, nil; want an error wrapping ErrMalformedEnvelope", msg)
			}
			if !errors.Is(err, ErrMalformedEnvelope) {
				t.Fatalf("fromRecord error = %v, want it to wrap ErrMalformedEnvelope", err)
			}
			if msg.ID != "" {
				t.Errorf("a malformed record must yield no message, got %+v", msg)
			}
			// The operator needs to find the record: partition and offset
			// are in the message.
			if got := err.Error(); !contains(got, "partition 2") || !contains(got, "offset 71") {
				t.Errorf("error does not locate the record: %q", got)
			}
		})
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
