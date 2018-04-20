package streamclient_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutils/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewProducer(t *testing.T) {
	t.Parallel()

	_, err := streamclient.NewProducer()
	assert.Error(t, err)
}

func TestIntegrationNewProducer_Env(t *testing.T) {
	testutils.Integration(t)

	var tests = []struct {
		env    string
		typeOf string
		opts   func(*streamconfig.Producer)
	}{
		{
			"standardstream",
			"*standardstreamclient.Producer",
			nil,
		},

		{
			"inmem",
			"*inmemclient.Producer",
			nil,
		},

		{
			"kafka",
			"*kafkaclient.Producer",
			func(c *streamconfig.Producer) {
				c.Kafka.Brokers = []string{kafkaclient.TestBrokerAddress}
				c.Kafka.Topic = "test"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			os.Setenv("STREAMCLIENT_PRODUCER", tt.env)
			defer os.Unsetenv("STREAMCLIENT_PRODUCER")

			producer, err := streamclient.NewProducer(tt.opts)
			require.NoError(t, err)

			assert.Equal(t, tt.typeOf, reflect.TypeOf(producer).String())
		})
	}
}

func TestIntegrationNewProducer_Pubsub(t *testing.T) {
	os.Setenv("STREAMCLIENT_PRODUCER", "pubsub")
	defer os.Unsetenv("STREAMCLIENT_PRODUCER")

	_, err := streamclient.NewProducer()
	require.Error(t, err)
}

func TestIntegrationNewProducer_Env_DryRun(t *testing.T) {
	os.Setenv("DRY_RUN", "1")
	defer os.Unsetenv("DRY_RUN")

	producer, err := streamclient.NewProducer()
	require.NoError(t, err)

	assert.Equal(t, "*standardstreamclient.Producer", reflect.TypeOf(producer).String())
}

func TestIntegrationNewProducer_Env_DryRun_Overridden(t *testing.T) {
	os.Setenv("STREAMCLIENT_PRODUCER", "inmem")
	defer os.Unsetenv("STREAMCLIENT_PRODUCER")

	os.Setenv("DRY_RUN", "1")
	defer os.Unsetenv("DRY_RUN")

	producer, err := streamclient.NewProducer()
	require.NoError(t, err)

	assert.Equal(t, "*inmemclient.Producer", reflect.TypeOf(producer).String())
}

func TestIntegrationNewProducer_Unknown(t *testing.T) {
	_, err := streamclient.NewProducer()
	assert.Error(t, err)
}
