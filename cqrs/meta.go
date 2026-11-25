package cqrs

import "context"

// Metadata is the cross-cutting context of the message that carried a
// command: trace propagation and the flag decisions pinned at the edge
// that started the flow (gohex/featureflag, ADR-0021). It never holds
// business data — that is what the command is for.
//
// It travels on the context rather than in the handler's signature
// because a command handler already takes a context, and because a
// handler that needs it is the exception: the [Consumer] installs it,
// and a handler reads it only when it must act on a decision taken
// upstream, like a payment handler honouring a flag the HTTP edge
// evaluated for the customer:
//
//	func (h *Handlers) CapturePayment(ctx context.Context, cmd CapturePayment) error {
//	    if featureflag.PinnedIn(cqrs.MetaFromContext(ctx), "fake-gateway") {
//	        ...
//	    }
//	}
//
// A command dispatched locally — an HTTP edge dispatching its own
// command — has no envelope, so MetaFromContext returns nil there. Nil
// is the safe answer: a flag absent from pinned metadata is off.
type metaKey struct{}

// WithMeta returns ctx carrying md as the current command's envelope
// metadata. The [Consumer] calls this for every consumed command;
// callers use it directly to test a handler that reads metadata.
func WithMeta(ctx context.Context, md map[string]string) context.Context {
	if len(md) == 0 {
		return ctx
	}
	return context.WithValue(ctx, metaKey{}, md)
}

// MetaFromContext returns the envelope metadata of the command being
// handled, or nil when the command did not arrive over the wire. Do not
// mutate the returned map.
func MetaFromContext(ctx context.Context) map[string]string {
	md, _ := ctx.Value(metaKey{}).(map[string]string)
	return md
}
