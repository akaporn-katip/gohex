package kafkabroker_test

import (
	"os"
	"strings"
	"testing"

	kafkabroker "github.com/akaporn-katip/gohex/libs/broker-kafka"
	"github.com/akaporn-katip/gohex/libs/broker/brokertest"
)

// TestKafkaBrokerContract runs the shared broker contract suite against
// a real Kafka. Point GOHEX_KAFKA_TEST_BROKERS at a disposable cluster,
// e.g.:
//
//	docker run -d --rm -p 9092:9092 apache/kafka:3.8.0
//	GOHEX_KAFKA_TEST_BROKERS=localhost:9092 go test ./...
func TestKafkaBrokerContract(t *testing.T) {
	seeds := os.Getenv("GOHEX_KAFKA_TEST_BROKERS")
	if seeds == "" {
		t.Skip("set GOHEX_KAFKA_TEST_BROKERS to run Kafka contract tests")
	}

	b, err := kafkabroker.New(strings.Split(seeds, ",")...)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(b.Close)

	brokertest.Run(t, func(t *testing.T) brokertest.PubSub {
		return b
	})
}
