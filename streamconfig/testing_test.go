package streamconfig_test

import (
	"os"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTestNewConsumer(t *testing.T) {
	c1, _ := streamconfig.NewConsumer()
	c2 := streamconfig.TestNewConsumer(t, true)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithoutDefaults(t *testing.T) {
	c1 := streamconfig.Consumer{}
	c2 := streamconfig.TestNewConsumer(t, false)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewConsumer_WithOptions(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	options := func(c *streamconfig.Consumer) {
		c.Logger = *logger
	}

	c1 := streamconfig.Consumer{Logger: *logger}
	c2 := streamconfig.TestNewConsumer(t, false, options)

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

	options := func(c *streamconfig.Consumer) {
		c.Kafka.Brokers = []string{"broker2"}
		c.Kafka.ID = "test"
	}

	c1 := streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Brokers: []string{"broker1"}, ID: "test"}}
	c2 := streamconfig.TestNewConsumer(t, false, options)

	assert.EqualValues(t, c1, c2)
}

func TestTestNewProducer(t *testing.T) {
	p1, _ := streamconfig.NewProducer()
	p2 := streamconfig.TestNewProducer(t, true)

	assert.EqualValues(t, p1, p2)
}

func TestTestNewProducer_WithoutDefaults(t *testing.T) {
	p1 := streamconfig.Producer{}
	p2 := streamconfig.TestNewProducer(t, false)

	assert.EqualValues(t, p1, p2)
}

func TestTestNewProducer_WithOptions(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	options := func(c *streamconfig.Producer) {
		c.Logger = *logger
	}

	p1 := streamconfig.Producer{Logger: *logger}
	p2 := streamconfig.TestNewProducer(t, false, options)

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

	options := func(c *streamconfig.Producer) {
		c.Kafka.Brokers = []string{"broker2"}
		c.Kafka.ID = "test"
	}

	c1 := streamconfig.Producer{Kafka: kafkaconfig.Producer{Brokers: []string{"broker1"}, ID: "test"}}
	c2 := streamconfig.TestNewProducer(t, false, options)

	assert.EqualValues(t, c1, c2)
}

func TestTestConsumerOptions(t *testing.T) {
	options := func(c *streamconfig.Consumer) {
		c.Kafka.GroupID = "overrideGroup"
	}

	opts := streamconfig.TestConsumerOptions(t, options)
	require.Len(t, opts, 2)

	c := &streamconfig.Consumer{}

	opts[0](c)
	assert.Equal(t, c.Kafka.GroupID, "testGroup")

	opts[1](c)
	assert.Equal(t, c.Kafka.GroupID, "overrideGroup")
}
