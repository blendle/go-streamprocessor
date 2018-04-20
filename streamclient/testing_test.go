package streamclient_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
)

func TestTestMessageFromConsumer(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- streammsg.TestMessage(t, "hello", "world")

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	message := streamclient.TestMessageFromConsumer(t, consumer)

	assert.Equal(t, "hello", string(message.Key))
	assert.Equal(t, "world", string(message.Value))
}
