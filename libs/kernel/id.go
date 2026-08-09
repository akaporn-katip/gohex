package kernel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// ErrInvalidID is returned by ParseID and ID.UnmarshalText for malformed
// input. It is a DomainError so edge adapters map it to a 4xx response
// automatically (ADR-0012).
var ErrInvalidID = NewDomainError("invalid_id", "malformed identifier")

// ID is a typed identifier for aggregate type T. Distinct type parameters
// yield distinct, non-convertible types, so passing an order's ID where a
// customer's is expected is a compile error:
//
//	type OrderID = kernel.ID[Order]
//	type CustomerID = kernel.ID[Customer]
//
// The zero value is invalid (parse, don't validate): obtain an ID only
// via NewID or ParseID. IDs are immutable, comparable (usable as map
// keys), and serialize as canonical lowercase UUID strings.
type ID[T any] struct {
	value string
}

// NewID generates a new random (version 4) UUID identifier.
func NewID[T any]() ID[T] {
	var b [16]byte
	rand.Read(b[:])         //nolint:errcheck // never fails; panics on broken entropy
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant
	return ID[T]{value: formatUUID(b)}
}

// ParseID validates s as a UUID and returns the typed identifier.
// Input is normalized to lowercase; malformed input yields ErrInvalidID.
func ParseID[T any](s string) (ID[T], error) {
	if len(s) != 36 {
		return ID[T]{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
	}
	s = strings.ToLower(s)
	for i := range 36 {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return ID[T]{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
			}
		default:
			if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
				return ID[T]{}, fmt.Errorf("%w: %q", ErrInvalidID, s)
			}
		}
	}
	return ID[T]{value: s}, nil
}

// IsZero reports whether the ID is the invalid zero value.
func (id ID[T]) IsZero() bool { return id.value == "" }

// String returns the canonical lowercase UUID form, or "" for the zero value.
func (id ID[T]) String() string { return id.value }

// MarshalText implements encoding.TextMarshaler (and thereby JSON
// serialization). Marshaling the zero value is an error: an entity with
// no identity must never be silently persisted or published.
func (id ID[T]) MarshalText() ([]byte, error) {
	if id.IsZero() {
		return nil, fmt.Errorf("%w: zero ID", ErrInvalidID)
	}
	return []byte(id.value), nil
}

// UnmarshalText implements encoding.TextUnmarshaler with ParseID semantics.
func (id *ID[T]) UnmarshalText(text []byte) error {
	parsed, err := ParseID[T](string(text))
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

func formatUUID(b [16]byte) string {
	var dst [36]byte
	hex.Encode(dst[:8], b[:4])
	dst[8] = '-'
	hex.Encode(dst[9:13], b[4:6])
	dst[13] = '-'
	hex.Encode(dst[14:18], b[6:8])
	dst[18] = '-'
	hex.Encode(dst[19:23], b[8:10])
	dst[23] = '-'
	hex.Encode(dst[24:], b[10:])
	return string(dst[:])
}
