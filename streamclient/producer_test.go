package streamclient_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	_, err := streamclient.NewProducer()
	assert.Error(t, err)
}

func TestIntegrationNewProducer_Env(t *testing.T) {
	testutil.Integration(t)

	var tests = []struct {
		env    string
		typeOf string
		opts   func(*streamconfig.Producer)
	}{
		{
			"standardstream",
			"*standardstreamclient.producer",
			nil,
		},

		{
			"inmem",
			"*inmemclient.producer",
			nil,
		},

		{
			"kafka",
			"*kafkaclient.producer",
			func(c *streamconfig.Producer) {
				c.Kafka.Brokers = []string{kafkaconfig.TestBrokerAddress}
				c.Kafka.Topic = "test"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			_ = os.Setenv("STREAMCLIENT_PRODUCER", tt.env)
			defer os.Unsetenv("STREAMCLIENT_PRODUCER") // nolint: errcheck

			producer, err := streamclient.NewProducer(tt.opts)
			require.NoError(t, err)

			assert.Equal(t, tt.typeOf, reflect.TypeOf(producer).String())
		})
	}
}

func TestNewProducer_Pubsub(t *testing.T) {
	_ = os.Setenv("STREAMCLIENT_PRODUCER", "pubsub")
	defer os.Unsetenv("STREAMCLIENT_PRODUCER") // nolint: errcheck

	_, err := streamclient.NewProducer()
	require.Error(t, err)
}

func TestNewProducer_Env_DryRun(t *testing.T) {
	_ = os.Setenv("DRY_RUN", "1")
	defer os.Unsetenv("DRY_RUN") // nolint: errcheck

	producer, err := streamclient.NewProducer()
	require.NoError(t, err)

	assert.Equal(t, "*standardstreamclient.producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_Env_DryRun_Overridden(t *testing.T) {
	_ = os.Setenv("STREAMCLIENT_PRODUCER", "inmem")
	defer os.Unsetenv("STREAMCLIENT_PRODUCER") // nolint: errcheck

	_ = os.Setenv("DRY_RUN", "1")
	defer os.Unsetenv("DRY_RUN") // nolint: errcheck

	producer, err := streamclient.NewProducer()
	require.NoError(t, err)

	assert.Equal(t, "*inmemclient.producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_Unknown(t *testing.T) {
	_, err := streamclient.NewProducer()
	assert.Error(t, err)
}
