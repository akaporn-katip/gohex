package broker

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryBroker is an in-process Publisher and Subscriber for tests and
// single-process wiring (ADR-0003). It honors the port contract:
// at-least-once delivery, per-group offsets starting at the earliest
// message, and in-order delivery per topic. One active member per
// (topic, group).
type MemoryBroker struct {
	mu      sync.Mutex
	topics  map[string][]Message
	offsets map[groupKey]int
	active  map[groupKey]bool
}

type groupKey struct{ topic, group string }

var (
	_ Publisher  = (*MemoryBroker)(nil)
	_ Subscriber = (*MemoryBroker)(nil)
)

func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{
		topics:  map[string][]Message{},
		offsets: map[groupKey]int{},
		active:  map[groupKey]bool{},
	}
}

func (b *MemoryBroker) Publish(_ context.Context, topic string, msgs ...Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.topics[topic] = append(b.topics[topic], msgs...)
	return nil
}

func (b *MemoryBroker) Subscribe(ctx context.Context, topic, group string, handler Handler) error {
	key := groupKey{topic, group}
	b.mu.Lock()
	if b.active[key] {
		b.mu.Unlock()
		return fmt.Errorf("broker: group %q already has an active member on topic %q", group, topic)
	}
	b.active[key] = true
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.active, key)
		b.mu.Unlock()
	}()

	const idlePoll = 2 * time.Millisecond
	retryBackoff := 5 * time.Millisecond
	for {
		if ctx.Err() != nil {
			return nil
		}
		b.mu.Lock()
		offset := b.offsets[key]
		log := b.topics[topic]
		var next *Message
		if offset < len(log) {
			m := log[offset]
			next = &m
		}
		b.mu.Unlock()

		if next == nil {
			if !sleepCtx(ctx, idlePoll) {
				return nil
			}
			continue
		}
		if err := handler(ctx, *next); err != nil {
			// At-least-once: redeliver the same message until it succeeds
			// or the subscriber shuts down.
			if !sleepCtx(ctx, retryBackoff) {
				return nil
			}
			continue
		}
		b.mu.Lock()
		b.offsets[key] = offset + 1
		b.mu.Unlock()
	}
}

// sleepCtx sleeps for d; false means ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
