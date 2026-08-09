// Package saga is the orchestration engine (ADR-0007): explicit,
// long-running workflows that react to integration events, issue
// commands to other services, and compensate on failure — with the whole
// flow readable in one place.
//
// A saga instance IS an event-sourced stream (category "saga.<name>",
// one instance per correlation ID). Three framework events make up the
// stream: the full envelope of every integration event received, every
// command decided, and the end marker. Because decisions are recorded as
// stream events, sending is the existing relay's job — appending the
// decision and dispatching the command are one atomic write
// ([RegisterCommandRouting] wires the routing).
//
// Handlers are pure decision functions: func(s *S, e E) (Decisions,
// error), no context, no I/O, no clock — they run both live and during
// replay (where returned commands are discarded because they were
// already recorded). Compensation needs no special machinery: a
// rejection event (PaymentFailedV1, StockRejectedV1) is handled like any
// other, deciding compensating commands.
//
// Incoming envelope IDs are part of the stream, so broker redeliveries
// deduplicate against the saga's own history.
package saga

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/cqrs"
	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/kernel"
	"github.com/akaporn-katip/gohex/libs/relay"
)

// OutCommand is a command a saga decided to send.
type OutCommand struct {
	// Topic is the target's command topic, e.g. "billing.commands".
	Topic string
	// Key orders the command relative to others (default: the saga's
	// correlation ID).
	Key string
	// Cmd is the command contract to serialize.
	Cmd cqrs.Command
}

// Decisions is a handler's output: commands to send and whether the
// saga instance is finished.
type Decisions struct {
	Commands []OutCommand
	// End marks the instance complete; later events for it are ignored.
	End bool
}

// Send is shorthand for a Decisions sending one or more commands.
func Send(cmds ...OutCommand) Decisions { return Decisions{Commands: cmds} }

// End is shorthand for a Decisions that only completes the saga.
func End() Decisions { return Decisions{End: true} }

// Definition describes one saga type: its name and the integration
// events it reacts to. Build it at startup with [New] and [OnEvent].
type Definition[S any] struct {
	name     string
	handlers map[string]*evHandler[S]
}

type evHandler[S any] struct {
	correlate func(msg broker.Message) (string, error)
	handle    func(s *S, msg broker.Message) (Decisions, error)
}

func New[S any](name string) *Definition[S] {
	if name == "" {
		panic("saga: New: empty name")
	}
	return &Definition[S]{name: name, handlers: map[string]*evHandler[S]{}}
}

// Name returns the saga's name; its stream category is "saga.<name>".
func (d *Definition[S]) Name() string { return d.name }

// Category returns the stream category saga instances live under.
func (d *Definition[S]) Category() string { return "saga." + d.name }

// OnEvent registers a reaction to integration event E, matched by
// contract name AND version. correlate derives the saga instance ID
// from the event (e.g. the order ID); handle decides. Handlers must be
// pure: no I/O, no clock — they re-run on every replay.
func OnEvent[S any, E broker.IntegrationEvent](d *Definition[S],
	correlate func(e E) string,
	handle func(s *S, e E) (Decisions, error),
) {
	var zero E
	key := eventKey(zero.EventName(), zero.ContractVersion())
	if _, dup := d.handlers[key]; dup {
		panic(fmt.Sprintf("saga %s: duplicate handler for %q", d.name, key))
	}
	decode := func(msg broker.Message) (E, error) {
		var e E
		if err := json.Unmarshal(msg.Payload, &e); err != nil {
			return e, fmt.Errorf("saga %s: decoding %s (message %s): %w", d.name, key, msg.ID, err)
		}
		return e, nil
	}
	d.handlers[key] = &evHandler[S]{
		correlate: func(msg broker.Message) (string, error) {
			e, err := decode(msg)
			if err != nil {
				return "", err
			}
			return correlate(e), nil
		},
		handle: func(s *S, msg broker.Message) (Decisions, error) {
			e, err := decode(msg)
			if err != nil {
				return Decisions{}, err
			}
			return handle(s, e)
		},
	}
}

func eventKey(name string, version int) string {
	return name + "@v" + strconv.Itoa(version)
}

// --- the saga's stream events (framework-owned) ---

// eventReceived records the full envelope of a consumed integration
// event; replaying it re-runs the handler to rebuild state.
type eventReceived struct {
	Envelope broker.Message `json:"envelope"`
}

func (eventReceived) EventName() string { return "saga.event_received" }

// commandRequested records a decision to send a command. The relay
// routes it to its topic ([RegisterCommandRouting]) — decide-and-send is
// one atomic append.
type commandRequested struct {
	Topic           string            `json:"topic"`
	Key             string            `json:"key"`
	CommandName     string            `json:"command_name"`
	ContractVersion int               `json:"contract_version"`
	Payload         json.RawMessage   `json:"payload"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

func (commandRequested) EventName() string { return "saga.command_requested" }

// ended marks the instance complete.
type ended struct{}

func (ended) EventName() string { return "saga.ended" }

// RegisterEvents registers the saga stream events with a service's event
// registry. Call once per registry, regardless of how many saga types
// the service hosts.
func RegisterEvents(r *eventstore.Registry) {
	eventstore.Register[eventReceived](r)
	eventstore.Register[commandRequested](r)
	eventstore.Register[ended](r)
}

// RegisterCommandRouting wires the relay to publish every recorded
// commandRequested to its embedded topic, with the deterministic message
// ID consumers dedup by. Call once per relay.
func RegisterCommandRouting(r *relay.Relay) {
	r.Route(commandRequested{}.EventName(),
		func(rec eventstore.RecordedEvent, e kernel.DomainEvent) (string, broker.Message, bool, error) {
			cr := e.(commandRequested)
			return cr.Topic, broker.Message{
				ID:         relay.MessageID(rec),
				Key:        cr.Key,
				Type:       cr.CommandName,
				Version:    cr.ContractVersion,
				OccurredAt: rec.OccurredAt,
				Payload:    cr.Payload,
				Metadata:   cr.Metadata,
			}, true, nil
		})
}

// --- the running instance (framework-owned aggregate) ---

// instance is the event-sourced aggregate holding one saga execution.
type instance[S any] struct {
	kernel.Root
	def   *Definition[S]
	state S
	ended bool
	seen  map[string]bool // envelope IDs already received
	// lastDecisions captures the handler output of the most recent
	// eventReceived Apply; live processing reads it, replay ignores it.
	lastDecisions Decisions
}

func newInstance[S any](def *Definition[S]) *instance[S] {
	return &instance[S]{def: def, seen: map[string]bool{}}
}

func (i *instance[S]) Apply(e kernel.DomainEvent) error {
	switch ev := e.(type) {
	case eventReceived:
		i.seen[ev.Envelope.ID] = true
		handler, ok := i.def.handlers[eventKey(ev.Envelope.Type, ev.Envelope.Version)]
		if !ok {
			// The handler existed when this was recorded; tolerate its
			// removal — the recorded commandRequested events remain the
			// authority on what was sent.
			i.lastDecisions = Decisions{}
			return nil
		}
		decisions, err := handler.handle(&i.state, ev.Envelope)
		if err != nil {
			return err
		}
		i.lastDecisions = decisions
		return nil
	case commandRequested:
		return nil // the decision itself; state already reflects it
	case ended:
		i.ended = true
		return nil
	default:
		return fmt.Errorf("saga %s: unknown stream event %q", i.def.name, e.EventName())
	}
}
