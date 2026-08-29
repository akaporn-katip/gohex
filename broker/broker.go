// Package broker defines the messaging port (ADR-0003): publish and
// subscribe with at-least-once delivery, plus the standard message
// envelope every payload travels in (ADR-0005). It depends only on the
// standard library; adapters (Kafka, in-memory) carry the heavy
// dependencies.
package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Message is the envelope on the wire (ADR-0005). The payload is a
// versioned integration-event or command contract as JSON; everything
// else is envelope.
type Message struct {
	// ID uniquely identifies the message. Publishers that may redeliver
	// (the relay) use deterministic IDs so consumers can deduplicate.
	ID string `json:"id"`
	// Key groups messages that must stay ordered relative to each other
	// (typically the aggregate ID); partitioned brokers use it for
	// partition assignment.
	Key string `json:"key"`
	// Type names the contract, e.g. "ordering.order_placed".
	Type string `json:"type"`
	// Version is the contract version — the 1 in OrderPlacedV1.
	Version    int             `json:"version"`
	OccurredAt time.Time       `json:"occurred_at"`
	Payload    json.RawMessage `json:"payload"`
	// Metadata carries cross-cutting context (trace propagation), never
	// business data.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// IntegrationEvent is a versioned public contract published to the
// broker (ADR-0004): deliberately boring, stable data. Contract types
// are suffixed with their version (OrderPlacedV1) and once published a
// version's shape never changes — evolution means a new version.
type IntegrationEvent interface {
	// EventName names the contract, e.g. "ordering.order_placed".
	EventName() string
	// ContractVersion is the published schema version, starting at 1.
	ContractVersion() int
}

// NewMessage wraps an integration event in an envelope.
func NewMessage(id, key string, occurredAt time.Time, e IntegrationEvent, metadata map[string]string) (Message, error) {
	payload, err := json.Marshal(e)
	if err != nil {
		return Message{}, fmt.Errorf("broker: encoding %s: %w", e.EventName(), err)
	}
	return Message{
		ID:         id,
		Key:        key,
		Type:       e.EventName(),
		Version:    e.ContractVersion(),
		OccurredAt: occurredAt,
		Payload:    payload,
		Metadata:   metadata,
	}, nil
}

// Publisher is the outbound port.
type Publisher interface {
	// Publish sends messages to a topic. On return with nil error every
	// message is durably accepted by the broker. Messages sharing a Key
	// are published in order.
	Publish(ctx context.Context, topic string, msgs ...Message) error
}

// Handler processes one message. Returning nil acknowledges it;
// returning an error causes redelivery (at-least-once) — so handlers
// must be idempotent, and a definitive business rejection must be
// expressed as a published fact, never a handler error (ADR-0012).
type Handler func(ctx context.Context, msg Message) error

// Subscriber is the inbound port.
type Subscriber interface {
	// Subscribe consumes a topic as a member of group, invoking handler
	// for each message. Each group receives every message; messages
	// sharing a Key are delivered in order within the group. Consumption
	// starts at the earliest retained message for a new group.
	//
	// Subscribe blocks: it returns nil after ctx is cancelled (clean
	// shutdown) or an error if the subscription fails.
	Subscribe(ctx context.Context, topic, group string, handler Handler) error
}
