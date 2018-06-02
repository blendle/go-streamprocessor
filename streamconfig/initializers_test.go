package streamconfig_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewConsumer()
	require.NoError(t, err)

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"streamconfig.Consumer", config},
		{"inmemconfig.Consumer", config.Inmem},
		{"kafkaconfig.Consumer", config.Kafka},
		{"pubsubconfig.Consumer", config.Pubsub},
		{"standardstreamconfig.Consumer", config.Standardstream},
		{"*zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewConsumer_WithOptions(t *testing.T) {
	config, err := streamconfig.NewConsumer(streamconfig.KafkaID("test"))
	require.NoError(t, err)

	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewConsumer_WithOptions_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewConsumer(nil)
	assert.NoError(t, err)
}

func TestNewConsumer_WithOptions_NilLogger(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewConsumer(streamconfig.Logger(nil))
	require.NoError(t, err)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}

func TestNewConsumer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewConsumer_WithEnvironmentVariables_CustomName(t *testing.T) {
	_ = os.Setenv("HELLO_CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("HELLO_CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(streamconfig.Name("hello"))
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewConsumer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewConsumer_WithOptionsWithoutEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(
		streamconfig.DisableEnvironmentConfig(),
		streamconfig.KafkaBroker("broker2"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker2"}, config.Kafka.Brokers)
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewProducer()
	require.NoError(t, err)

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"streamconfig.Producer", config},
		{"inmemconfig.Producer", config.Inmem},
		{"kafkaconfig.Producer", config.Kafka},
		{"pubsubconfig.Producer", config.Pubsub},
		{"standardstreamconfig.Producer", config.Standardstream},
		{"*zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewProducer_WithOptions(t *testing.T) {
	config, err := streamconfig.NewProducer(streamconfig.KafkaID("test"))
	require.NoError(t, err)

	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewProducer_WithOptions_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewProducer(nil)
	assert.NoError(t, err)
}

func TestNewProducer_WithOptions_NilLogger(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewProducer(streamconfig.Logger(nil))
	require.NoError(t, err)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}

func TestNewProducer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewProducer_WithEnvironmentVariables_CustomName(t *testing.T) {
	_ = os.Setenv("HELLO_WORLD_PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("HELLO_WORLD_PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(streamconfig.Name("hello_world"))
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewProducer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewProducer_WithOptionsWithoutEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(
		streamconfig.DisableEnvironmentConfig(),
		streamconfig.KafkaBroker("broker2"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker2"}, config.Kafka.Brokers)
}
