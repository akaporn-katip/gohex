package eventstore_test

import (
	"testing"

	"github.com/akaporn-katip/gohex/eventstore"
	"github.com/akaporn-katip/gohex/eventstore/storetest"
)

func TestMemoryCheckpointStoreContract(t *testing.T) {
	storetest.RunCheckpoints(t, func(t *testing.T) eventstore.CheckpointStore {
		return eventstore.NewMemoryCheckpointStore()
	})
}
