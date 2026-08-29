// Package kafkabroker is the Kafka adapter for the broker port
// (ADR-0003), built on franz-go. Envelope fields travel as record
// headers; the payload is the record value; Message.Key is the record
// key, so per-key ordering maps onto Kafka partition ordering.
package kafkabroker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/akaporn-katip/gohex/broker"
)

// Broker implements broker.Publisher; Subscribe creates its own
// consumer-group client per call, so one Broker serves any number of
// subscriptions.
type Broker struct {
	seeds    []string
	producer *kgo.Client
}

var (
	_ broker.Publisher  = (*Broker)(nil)
	_ broker.Subscriber = (*Broker)(nil)
)

// New connects a producer to the given seed brokers.
func New(seeds ...string) (*Broker, error) {
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafkabroker: %w", err)
	}
	return &Broker{seeds: seeds, producer: producer}, nil
}

// Close releases the producer client.
func (b *Broker) Close() { b.producer.Close() }

func (b *Broker) Publish(ctx context.Context, topic string, msgs ...broker.Message) error {
	records := make([]*kgo.Record, len(msgs))
	for i, m := range msgs {
		rec, err := toRecord(topic, m)
		if err != nil {
			return err
		}
		records[i] = rec
	}
	if err := b.producer.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafkabroker: publish to %s: %w", topic, err)
	}
	return nil
}

// retryBackoff paces redelivery of a message whose handler failed.
const retryBackoff = 250 * time.Millisecond

func (b *Broker) Subscribe(ctx context.Context, topic, group string, handler broker.Handler) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(b.seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("kafkabroker: subscribe %s/%s: %w", topic, group, err)
	}
	defer client.Close()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if fetches.IsClientClosed() {
			return nil
		}
		for _, fetchErr := range fetches.Errors() {
			return fmt.Errorf("kafkabroker: fetch %s/%s: %w", fetchErr.Topic, group, fetchErr.Err)
		}

		var failed error
		fetches.EachRecord(func(rec *kgo.Record) {
			if failed != nil {
				return
			}
			msg := fromRecord(rec)
			// At-least-once: retry this message until it succeeds or we
			// shut down; the offset is only committed after success.
			for {
				if err := handler(ctx, msg); err == nil {
					break
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(retryBackoff):
				}
			}
			if err := client.CommitRecords(ctx, rec); err != nil {
				failed = err
			}
		})
		if ctx.Err() != nil {
			return nil
		}
		if failed != nil {
			return fmt.Errorf("kafkabroker: commit %s/%s: %w", topic, group, failed)
		}
	}
}

const (
	headerID         = "gohex-id"
	headerType       = "gohex-type"
	headerVersion    = "gohex-version"
	headerOccurredAt = "gohex-occurred-at"
	headerMetadata   = "gohex-metadata"
)

func toRecord(topic string, m broker.Message) (*kgo.Record, error) {
	headers := []kgo.RecordHeader{
		{Key: headerID, Value: []byte(m.ID)},
		{Key: headerType, Value: []byte(m.Type)},
		{Key: headerVersion, Value: []byte(strconv.Itoa(m.Version))},
		{Key: headerOccurredAt, Value: []byte(m.OccurredAt.UTC().Format(time.RFC3339Nano))},
	}
	if len(m.Metadata) > 0 {
		metadata, err := json.Marshal(m.Metadata)
		if err != nil {
			return nil, fmt.Errorf("kafkabroker: encoding metadata of %s: %w", m.ID, err)
		}
		headers = append(headers, kgo.RecordHeader{Key: headerMetadata, Value: metadata})
	}
	return &kgo.Record{
		Topic:   topic,
		Key:     []byte(m.Key),
		Value:   m.Payload,
		Headers: headers,
	}, nil
}

func fromRecord(rec *kgo.Record) broker.Message {
	msg := broker.Message{Key: string(rec.Key), Payload: rec.Value}
	for _, h := range rec.Headers {
		switch h.Key {
		case headerID:
			msg.ID = string(h.Value)
		case headerType:
			msg.Type = string(h.Value)
		case headerVersion:
			msg.Version, _ = strconv.Atoi(string(h.Value))
		case headerOccurredAt:
			msg.OccurredAt, _ = time.Parse(time.RFC3339Nano, string(h.Value))
		case headerMetadata:
			_ = json.Unmarshal(h.Value, &msg.Metadata)
		}
	}
	return msg
}
