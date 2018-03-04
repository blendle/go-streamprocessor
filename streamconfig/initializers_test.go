package streamconfig_test

import (
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
		{"zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewConsumer_Options_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewConsumer(nil)
	assert.NoError(t, err)
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
		{"zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewProducer_Options_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewProducer(nil)
	assert.NoError(t, err)
}
