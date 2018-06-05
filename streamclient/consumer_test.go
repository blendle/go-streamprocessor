package streamclient_test

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumer(t *testing.T) {
	_, err := streamclient.NewConsumer()
	assert.Error(t, err)
}

func TestIntegrationNewConsumer_Env(t *testing.T) {
	testutil.Integration(t)

	var tests = []struct {
		env    string
		typeOf string
		opts   streamconfig.Option
	}{
		{
			"standardstream",
			"*standardstreamclient.consumer",
			nil,
		},

		{
			"inmem",
			"*inmemclient.consumer",
			nil,
		},

		{
			"kafka",
			"*kafkaclient.consumer",
			streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
				c.Kafka.Brokers = []string{kafkaconfig.TestBrokerAddress}
				c.Kafka.Topics = []string{"test"}
				c.Kafka.GroupID = "test"
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			_ = os.Setenv("CONSUMER_CLIENT_TYPE", tt.env)
			defer os.Unsetenv("CONSUMER_CLIENT_TYPE") // nolint: errcheck

			consumer, err := streamclient.NewConsumer(tt.opts)
			require.NoError(t, err)

			assert.Equal(t, tt.typeOf, reflect.TypeOf(consumer).String())
		})
	}
}

func TestNewConsumer_Unknown(t *testing.T) {
	_ = os.Setenv("CONSUMER_CLIENT_TYPE", "pubsub")
	defer os.Unsetenv("CONSUMER_CLIENT_TYPE") // nolint: errcheck

	_, err := streamclient.NewConsumer()
	assert.Error(t, err)
}

func TestNewConsumer_PipedData(t *testing.T) {
	if os.Getenv("BE_TESTING_STDIN") == "1" {
		consumer, err := streamclient.NewConsumer()
		require.NoError(t, err)

		require.Equal(t, "*standardstreamclient.consumer", reflect.TypeOf(consumer).String())
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), "BE_TESTING_STDIN=1")

	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	defer func() { assert.NoError(t, stdin.Close()) }()

	_, err = io.WriteString(stdin, `{ "hello": "world" }`)
	require.NoError(t, err)

	b, err := cmd.CombinedOutput()
	if e, ok := err.(*exec.ExitError); ok {
		assert.True(t, e.Success(), fmt.Sprintf("%s\n\n%s", e.String(), string(b)))
	}
}
