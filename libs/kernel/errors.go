package kernel

import "errors"

// DomainError is a business-rule violation: a definitive rejection that
// must never be retried. The command bus and the REST edge map it to a
// rejection integration event or a 4xx response; every other error is
// treated as infrastructure and retried (ADR-0012).
//
// Domains declare sentinel rules once and return them from behavior:
//
//	var ErrInsufficientStock = kernel.NewDomainError(
//		"insufficient_stock", "not enough stock to reserve")
//
// Callers match with errors.Is(err, ErrInsufficientStock), which compares
// by Code and therefore survives wrapping with fmt.Errorf("...: %w", err).
type DomainError struct {
	// Code identifies the rule in snake_case, e.g. "insufficient_stock".
	// It is part of the public contract (rejection events carry it).
	Code string
	// Message is a human-readable explanation. It is not part of the
	// contract and may change freely.
	Message string
}

// NewDomainError declares a domain error sentinel.
func NewDomainError(code, message string) *DomainError {
	return &DomainError{Code: code, Message: message}
}

func (e *DomainError) Error() string { return e.Code + ": " + e.Message }

// Is reports whether target is a DomainError with the same Code, making
// errors.Is match sentinels regardless of Message or wrapping.
func (e *DomainError) Is(target error) bool {
	var t *DomainError
	return errors.As(target, &t) && t.Code == e.Code
}

// AsDomainError unwraps err looking for a DomainError. Buses and edges
// use it to decide reject-vs-retry: a DomainError is final; anything
// else is infrastructure and retryable.
func AsDomainError(err error) (*DomainError, bool) {
	var de *DomainError
	ok := errors.As(err, &de)
	return de, ok
}
