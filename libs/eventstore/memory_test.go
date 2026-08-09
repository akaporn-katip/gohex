package eventstore_test

import (
	"testing"

	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/eventstore/storetest"
)

func TestMemoryStoreContract(t *testing.T) {
	storetest.Run(t, func(t *testing.T) eventstore.Store {
		return eventstore.NewMemoryStore()
	})
}
