package streamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewClient()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"streamconfig.Client", config},
		{"inmemconfig.Client", config.Inmem},
		{"kafkaconfig.Client", config.Kafka},
		{"pubsubconfig.Client", config.Pubsub},
		{"standardstreamconfig.Client", config.Standardstream},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	cc, err := streamconfig.NewClient()
	require.NoError(t, err)

	config, err := streamconfig.NewConsumer(cc)
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
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	cc, err := streamconfig.NewClient()
	require.NoError(t, err)

	config, err := streamconfig.NewProducer(cc)
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
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}
