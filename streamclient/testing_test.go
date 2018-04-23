package streamclient_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutil/inmemstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestMessageFromConsumer(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- stream.TestMessage(t, "hello", "world")

	opts := func(c *streamconfig.Consumer) {
		c.Inmem.ConsumeOnce = false
	}

	consumer, closer := inmemclient.TestConsumer(t, store, opts)
	defer closer()

	message := streamclient.TestMessageFromConsumer(t, consumer)

	assert.Equal(t, "hello", string(message.Key))
	assert.Equal(t, "world", string(message.Value))
}

func TestTestMessageFromConsumer_Timeout(t *testing.T) {
	t.Parallel()

	if os.Getenv("BE_TESTING_FATAL") == "1" {
		opts := func(c *streamconfig.Consumer) {
			c.Inmem.ConsumeOnce = false
		}

		consumer, closer := inmemclient.TestConsumer(t, nil, opts)
		defer closer()

		_ = streamclient.TestMessageFromConsumer(t, consumer)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

	out, err := cmd.CombinedOutput()
	require.NotNil(t, err, "output received: %s", string(out))

	assert.False(t, err.(*exec.ExitError).Success())
	assert.Contains(t, string(out), "Timeout while waiting for message to be returned.")
}
