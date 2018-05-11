package streamclient_test

import (
	"os"
	"os/exec"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestMessageFromConsumer(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- stream.TestMessage(t, "hello", "world")

	consumer, closer := inmemclient.TestConsumer(t, store, streamconfig.InmemListen())
	defer closer()

	message := streamclient.TestMessageFromConsumer(t, consumer)

	assert.Equal(t, "hello", string(message.Key))
	assert.Equal(t, "world", string(message.Value))
}

func TestTestMessageFromConsumer_Timeout(t *testing.T) {
	if os.Getenv("RUN_WITH_EXEC") == "1" {
		consumer, closer := inmemclient.TestConsumer(t, nil, streamconfig.InmemListen())
		defer closer()

		testutil.WithMultiplier(t, 0.01, func(tb testing.TB) {
			_ = streamclient.TestMessageFromConsumer(tb, consumer)
		})

		return
	}

	out, err := testutil.Exec(t, "", nil)

	require.NotNil(t, err)
	assert.False(t, err.(*exec.ExitError).Success())
	assert.Contains(t, out, "Timeout while waiting for message to be returned.")
}
