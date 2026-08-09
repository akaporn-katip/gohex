package eventstore_test

import (
	"testing"

	"github.com/akaporn-katip/gohex/libs/eventstore"
	"github.com/akaporn-katip/gohex/libs/eventstore/storetest"
)

func TestMemoryCheckpointStoreContract(t *testing.T) {
	storetest.RunCheckpoints(t, func(t *testing.T) eventstore.CheckpointStore {
		return eventstore.NewMemoryCheckpointStore()
	})
}
