package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/kernel"
)

// This file is the wire side of ADR-0008: commands travel between
// services as messages on point-to-point command topics
// ("<service>.commands"), consumed by the target's command bus with
// dedup by command ID.

// VersionedCommand optionally declares a contract version for a command
// that crosses service boundaries. Commands without it are version 1.
type VersionedCommand interface {
	Command
	ContractVersion() int
}

// NewCommandMessage wraps a command in the standard envelope. id must be
// deterministic per logical command (sagas derive it from their stream
// position) so consumers can deduplicate redeliveries; key groups
// commands that must stay ordered (typically the target aggregate's ID).
func NewCommandMessage(id, key string, occurredAt time.Time, cmd Command, metadata map[string]string) (broker.Message, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return broker.Message{}, fmt.Errorf("cqrs: encoding %s: %w", cmd.CommandName(), err)
	}
	version := 1
	if vc, ok := cmd.(VersionedCommand); ok {
		version = vc.ContractVersion()
	}
	return broker.Message{
		ID:         id,
		Key:        key,
		Type:       cmd.CommandName(),
		Version:    version,
		OccurredAt: occurredAt,
		Payload:    payload,
		Metadata:   metadata,
	}, nil
}

// Registry maps wire command names to Go command types.
type Registry struct {
	decoders map[string]func([]byte) (Command, error)
}

func NewRegistry() *Registry {
	return &Registry{decoders: map[string]func([]byte) (Command, error){}}
}

// RegisterCommand registers command type C under its CommandName.
// Panics on a duplicate or empty name (wiring bug).
func RegisterCommand[C Command](r *Registry) {
	var zero C
	name := zero.CommandName()
	if name == "" {
		panic("cqrs: RegisterCommand: empty command name")
	}
	if _, dup := r.decoders[name]; dup {
		panic(fmt.Sprintf("cqrs: RegisterCommand: duplicate command name %q", name))
	}
	r.decoders[name] = func(payload []byte) (Command, error) {
		var c C
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, fmt.Errorf("cqrs: decoding %s: %w", name, err)
		}
		return c, nil
	}
}

// Decode turns a wire message back into a command.
func (r *Registry) Decode(msg broker.Message) (Command, error) {
	decode, ok := r.decoders[msg.Type]
	if !ok {
		return nil, fmt.Errorf("cqrs: unregistered command %q (message %s)", msg.Type, msg.ID)
	}
	return decode(msg.Payload)
}

// Deduplicator remembers processed message IDs so redeliveries of an
// already-executed command are acknowledged without re-executing.
type Deduplicator interface {
	// Processed reports whether id has been marked.
	Processed(ctx context.Context, id string) (bool, error)
	// MarkProcessed records id. Marking an already-marked id is a no-op.
	MarkProcessed(ctx context.Context, id string) error
}

// MemoryDeduplicator is an in-process Deduplicator for tests and
// single-process wiring.
type MemoryDeduplicator struct {
	mu   sync.Mutex
	seen map[string]bool
}

var _ Deduplicator = (*MemoryDeduplicator)(nil)

func NewMemoryDeduplicator() *MemoryDeduplicator {
	return &MemoryDeduplicator{seen: map[string]bool{}}
}

func (d *MemoryDeduplicator) Processed(_ context.Context, id string) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.seen[id], nil
}

func (d *MemoryDeduplicator) MarkProcessed(_ context.Context, id string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen[id] = true
	return nil
}

// ConsumerConfig tunes a Consumer.
type ConsumerConfig struct {
	// OnRejected is invoked when a message is definitively not
	// processable: its command was rejected with a DomainError, or the
	// message could not be decoded. The message is acknowledged either
	// way — redelivery cannot fix a definitive rejection (ADR-0012) —
	// so wire this to record a rejection fact or alert. nil drops
	// rejections silently.
	OnRejected func(ctx context.Context, msg broker.Message, err error)
}

// Consumer bridges a broker subscription to a command bus:
//
//	consumer := cqrs.NewConsumer(bus, registry, dedup, cqrs.ConsumerConfig{...})
//	err := consumer.Run(ctx, subscriber, "billing.commands", "billing")
//
// Delivery semantics: at-least-once from the broker, narrowed by the
// Deduplicator — a crash after the handler succeeds but before the mark
// re-executes the command, so handlers must still be safe to replay.
// Definitive rejections are acknowledged (see ConsumerConfig.OnRejected);
// any other handler error is returned to the broker for redelivery.
type Consumer struct {
	bus      *Bus
	registry *Registry
	dedup    Deduplicator
	cfg      ConsumerConfig
}

func NewConsumer(bus *Bus, registry *Registry, dedup Deduplicator, cfg ConsumerConfig) *Consumer {
	return &Consumer{bus: bus, registry: registry, dedup: dedup, cfg: cfg}
}

// Run subscribes and consumes until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context, sub broker.Subscriber, topic, group string) error {
	return sub.Subscribe(ctx, topic, group, c.Handler())
}

// Handler returns the broker.Handler for callers that manage their own
// subscription.
func (c *Consumer) Handler() broker.Handler {
	return func(ctx context.Context, msg broker.Message) error {
		seen, err := c.dedup.Processed(ctx, msg.ID)
		if err != nil {
			return err
		}
		if seen {
			return nil
		}

		cmd, err := c.registry.Decode(msg)
		if err != nil {
			c.reject(ctx, msg, err)
			return c.dedup.MarkProcessed(ctx, msg.ID)
		}

		if err := c.bus.Dispatch(ctx, cmd); err != nil {
			if _, definitive := kernel.AsDomainError(err); definitive {
				c.reject(ctx, msg, err)
				return c.dedup.MarkProcessed(ctx, msg.ID)
			}
			return err // transient: broker redelivers
		}
		return c.dedup.MarkProcessed(ctx, msg.ID)
	}
}

func (c *Consumer) reject(ctx context.Context, msg broker.Message, err error) {
	if c.cfg.OnRejected != nil {
		c.cfg.OnRejected(ctx, msg, err)
	}
}
