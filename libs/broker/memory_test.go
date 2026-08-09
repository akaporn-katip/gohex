package broker_test

import (
	"context"
	"testing"

	"github.com/akaporn-katip/gohex/libs/broker"
	"github.com/akaporn-katip/gohex/libs/broker/brokertest"
)

func TestMemoryBrokerContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T) brokertest.PubSub {
		return broker.NewMemoryBroker()
	})
}

func TestMemoryBrokerRejectsDuplicateGroupMember(t *testing.T) {
	b := broker.NewMemoryBroker()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Prove the first member is registered by having it consume a message.
	consumed := make(chan struct{})
	go func() {
		_ = b.Subscribe(ctx, "t", "g", func(context.Context, broker.Message) error {
			close(consumed)
			return nil
		})
	}()
	if err := b.Publish(ctx, "t", broker.Message{ID: "m1"}); err != nil {
		t.Fatal(err)
	}
	<-consumed

	if err := b.Subscribe(ctx, "t", "g", func(context.Context, broker.Message) error { return nil }); err == nil {
		t.Fatal("second member of same group subscribed without error")
	}
}
