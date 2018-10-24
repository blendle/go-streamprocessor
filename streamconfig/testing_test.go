package streamconfig_test

import (
	"os"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestTestNewConsumer(t *testing.T) {
	c1, _ := streamconfig.NewConsumer()
	c1.Logger = nil

	c2 := streamconfig.TestNewConsumer(t, true)
	c2.Logger = nil

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithoutDefaults(t *testing.T) {
	c1 := streamconfig.Consumer{}
	c2 := streamconfig.TestNewConsumer(t, false)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithOptions(t *testing.T) {
	logger := zaptest.NewLogger(t)
	c1 := streamconfig.Consumer{Global: streamconfig.Global{Logger: logger}}
	c2 := streamconfig.TestNewConsumer(t, false, streamconfig.Logger(logger))

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("KAFKA_BROKERS") // nolint: errcheck

	c1 := streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Brokers: []string{"broker1"}}}
	c2 := streamconfig.TestNewConsumer(t, false)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("KAFKA_BROKERS") // nolint: errcheck

	options := []streamconfig.Option{
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	}

	c1 := streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Brokers: []string{"broker1"}, ID: "test"}}
	c2 := streamconfig.TestNewConsumer(t, false, options...)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewProducer(t *testing.T) {
	p1, _ := streamconfig.NewProducer()
	p1.Logger = nil

	p2 := streamconfig.TestNewProducer(t, true)
	p2.Logger = nil

	assert.EqualValues(t, p1, p2)
}

func TestTestNewProducer_WithoutDefaults(t *testing.T) {
	p1 := streamconfig.Producer{}
	p2 := streamconfig.TestNewProducer(t, false)

	assert.EqualValues(t, p1, p2)
}

func TestTestNewProducer_WithOptions(t *testing.T) {
	logger := zaptest.NewLogger(t)
	p1 := streamconfig.Producer{Global: streamconfig.Global{Logger: logger}}
	p2 := streamconfig.TestNewProducer(t, false, streamconfig.Logger(logger))

	assert.EqualValues(t, p1, p2)
}

func TestTestNewProducer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("KAFKA_BROKERS") // nolint: errcheck

	c1 := streamconfig.Producer{Kafka: kafkaconfig.Producer{Brokers: []string{"broker1"}}}
	c2 := streamconfig.TestNewProducer(t, false)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewProducer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("KAFKA_BROKERS") // nolint: errcheck

	options := []streamconfig.Option{
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	}

	c1 := streamconfig.Producer{Kafka: kafkaconfig.Producer{Brokers: []string{"broker1"}, ID: "test"}}
	c2 := streamconfig.TestNewProducer(t, false, options...)

	assert.EqualValues(t, c1, c2)
}

func TestTestConsumerOptions(t *testing.T) {
	opts := streamconfig.TestConsumerOptions(t, streamconfig.KafkaGroupID("overrideGroup"))
	require.Len(t, opts, 2)
}
