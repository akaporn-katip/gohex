package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/akaporn-katip/gohex/broker"
	"github.com/akaporn-katip/gohex/cqrs"
	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/kernel"
)

// Runner drives one saga definition: it consumes integration events
// from broker topics, loads (or starts) the correlated instance,
// records the received envelope plus the decided commands in one atomic
// append, and lets the relay do the sending.
type Runner[S any] struct {
	def  *Definition[S]
	repo *eventstore.Repository[*instance[S]]
	// conflictRetries bounds optimistic-concurrency retries before
	// handing the message back to the broker.
	conflictRetries int
}

// NewRunner wires a runner. The registry must have [RegisterEvents]
// applied.
func NewRunner[S any](def *Definition[S], store eventstore.Store, registry *eventstore.Registry) *Runner[S] {
	repo := eventstore.NewRepository(store, registry, def.Category(),
		func() *instance[S] { return newInstance(def) })
	return &Runner[S]{def: def, repo: repo, conflictRetries: 3}
}

// rawID satisfies the repository's id parameter.
type rawID string

func (r rawID) String() string { return string(r) }

// Handler returns the broker.Handler that drives the saga. Ack rules:
// events this saga doesn't handle, duplicates, events for ended
// instances, and undecodable payloads are acknowledged; storage errors
// are returned for redelivery.
func (r *Runner[S]) Handler() broker.Handler {
	return func(ctx context.Context, msg broker.Message) error {
		handler, ok := r.def.handlers[eventKey(msg.Type, msg.Version)]
		if !ok {
			return nil // not this saga's event
		}
		id, err := handler.correlate(msg)
		if err != nil {
			return nil // undecodable payload: redelivery cannot fix it
		}
		if id == "" {
			return nil
		}

		var lastErr error
		for attempt := 0; attempt <= r.conflictRetries; attempt++ {
			done, err := r.process(ctx, id, msg)
			if err == nil || done {
				return err
			}
			lastErr = err
		}
		return lastErr
	}
}

// process runs one delivery against the current instance. done=false
// with an error means a version conflict worth retrying.
func (r *Runner[S]) process(ctx context.Context, id string, msg broker.Message) (done bool, err error) {
	inst, err := r.repo.Load(ctx, rawID(id))
	if errors.Is(err, eventstore.ErrNotFound) {
		inst = newInstance(r.def)
	} else if err != nil {
		return true, err
	}

	if inst.ended || inst.seen[msg.ID] {
		return true, nil
	}

	// Decide: Apply runs the handler, mutating state and capturing the
	// decisions.
	if err := kernel.Raise(inst, eventReceived{Envelope: msg}); err != nil {
		return true, err
	}
	decisions := inst.lastDecisions

	// Trace context for outgoing commands: prefer the ambient context
	// (which includes the saga's own consumer span when o11y is wired),
	// falling back to the triggering event's metadata.
	cmdMetadata := eventstore.MetadataFromContext(ctx)
	if cmdMetadata == nil {
		cmdMetadata = msg.Metadata
	}

	for _, out := range decisions.Commands {
		if out.Topic == "" {
			return true, fmt.Errorf("saga %s/%s: command %s has no topic", r.def.name, id, out.Cmd.CommandName())
		}
		payload, err := json.Marshal(out.Cmd)
		if err != nil {
			return true, fmt.Errorf("saga %s/%s: encoding %s: %w", r.def.name, id, out.Cmd.CommandName(), err)
		}
		version := 1
		if vc, ok := out.Cmd.(cqrs.VersionedCommand); ok {
			version = vc.ContractVersion()
		}
		key := out.Key
		if key == "" {
			key = id
		}
		if err := kernel.Raise(inst, commandRequested{
			Topic:           out.Topic,
			Key:             key,
			CommandName:     out.Cmd.CommandName(),
			ContractVersion: version,
			Payload:         payload,
			Metadata:        cmdMetadata,
		}); err != nil {
			return true, err
		}
	}
	if decisions.End {
		if err := kernel.Raise(inst, ended{}); err != nil {
			return true, err
		}
	}

	// One atomic append: the received envelope, every decision, and the
	// end marker live or die together.
	if err := r.repo.Save(ctx, rawID(id), inst); err != nil {
		if errors.Is(err, eventstore.ErrVersionConflict) {
			return false, err // reload and re-decide
		}
		return true, err
	}
	return true, nil
}

// Run consumes the given topics as one group until ctx is cancelled
// (returns nil) or any subscription fails (returns the first error).
func (r *Runner[S]) Run(ctx context.Context, sub broker.Subscriber, group string, topics ...string) error {
	if len(topics) == 0 {
		return fmt.Errorf("saga %s: no topics to consume", r.def.name)
	}
	handler := r.Handler()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, len(topics))
	for _, topic := range topics {
		wg.Go(func() {
			if err := sub.Subscribe(ctx, topic, group, handler); err != nil {
				errc <- fmt.Errorf("saga %s: topic %s: %w", r.def.name, topic, err)
				cancel()
			}
		})
	}
	wg.Wait()
	close(errc)
	return <-errc
}
