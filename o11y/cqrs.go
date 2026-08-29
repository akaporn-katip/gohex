package o11y

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/akaporn-katip/gohex/cqrs"
	"github.com/akaporn-katip/gohex/kernel"
)

// CommandMiddleware traces command dispatch. A DomainError is an
// expected outcome — the span records the rejection code but is NOT
// marked as an error; only infrastructure failures are (ADR-0012, in
// trace form).
func CommandMiddleware() cqrs.Middleware {
	return func(next cqrs.HandlerFunc) cqrs.HandlerFunc {
		return func(ctx context.Context, cmd cqrs.Command) error {
			ctx, span := tracer().Start(ctx, "command "+cmd.CommandName())
			defer span.End()

			err := next(ctx, cmd)
			if err != nil {
				if de, ok := kernel.AsDomainError(err); ok {
					span.SetAttributes(attribute.String("gohex.rejection_code", de.Code))
				} else {
					span.RecordError(err)
					span.SetStatus(codes.Error, err.Error())
				}
			}
			return err
		}
	}
}

// QueryMiddleware traces query dispatch.
func QueryMiddleware() cqrs.QueryMiddleware {
	return func(next cqrs.QueryHandlerFunc) cqrs.QueryHandlerFunc {
		return func(ctx context.Context, q cqrs.Query) (any, error) {
			ctx, span := tracer().Start(ctx, "query "+q.QueryName())
			defer span.End()

			result, err := next(ctx, q)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			return result, err
		}
	}
}
