package kernel_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/akaporn-katip/gohex/libs/kernel"
)

var errStockRule = kernel.NewDomainError("insufficient_stock", "not enough stock to reserve")

func TestErrorsIsMatchesByCode(t *testing.T) {
	returned := kernel.NewDomainError("insufficient_stock", "wanted 5, have 2")

	if !errors.Is(returned, errStockRule) {
		t.Error("same code, different message: errors.Is = false, want true")
	}

	wrapped := fmt.Errorf("reserving for order 42: %w", returned)
	if !errors.Is(wrapped, errStockRule) {
		t.Error("wrapped: errors.Is = false, want true")
	}

	other := kernel.NewDomainError("payment_declined", "card declined")
	if errors.Is(other, errStockRule) {
		t.Error("different code: errors.Is = true, want false")
	}
}

func TestAsDomainError(t *testing.T) {
	wrapped := fmt.Errorf("context: %w", errStockRule)
	de, ok := kernel.AsDomainError(wrapped)
	if !ok {
		t.Fatal("AsDomainError(wrapped) = false, want true")
	}
	if de.Code != "insufficient_stock" {
		t.Errorf("Code = %q, want insufficient_stock", de.Code)
	}

	if _, ok := kernel.AsDomainError(errors.New("connection refused")); ok {
		t.Error("AsDomainError(infra error) = true, want false — it would be dropped instead of retried")
	}
	if _, ok := kernel.AsDomainError(nil); ok {
		t.Error("AsDomainError(nil) = true, want false")
	}
}

func TestErrorString(t *testing.T) {
	if got := errStockRule.Error(); got != "insufficient_stock: not enough stock to reserve" {
		t.Errorf("Error() = %q", got)
	}
}
