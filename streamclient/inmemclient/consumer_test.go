package inmemclient_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Consumer{}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	consumer, err := inmemclient.NewConsumer()
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	assert.Equal(t, "*inmemclient.Consumer", reflect.TypeOf(consumer).String())
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	options := func(c *streamconfig.Consumer) {
		c.Inmem.Store = store
	}

	consumer, err := inmemclient.NewConsumer(options)
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	assert.Equal(t, store, consumer.Config().Inmem.Store)
}

func TestNewConsumer_Messages(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	store.Add(streammsg.TestMessage(t, "key1", "hello world"))
	store.Add(streammsg.TestMessage(t, "key2", "hello universe!"))

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	msg := <-consumer.Messages()
	assert.Equal(t, "hello world", string(msg.Value))

	msg = <-consumer.Messages()
	assert.Equal(t, "hello universe!", string(msg.Value))

	_, ok := <-consumer.Messages()
	assert.False(t, ok, "consumer did not close after last message")
}

func TestConsumer_Messages_Ordering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.New()

	for i := 0; i < messageCount; i++ {
		store.Add(streammsg.TestMessage(t, strconv.Itoa(i), "hello world"+strconv.Itoa(i)))
	}

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		require.Equal(t, "hello world"+strconv.Itoa(i), string(msg.Value))
		require.Equal(t, strconv.Itoa(i), string(msg.Key))

		i++
	}

	assert.Equal(t, messageCount, i)
}

func TestConsumer_Messages_PerMessageMemoryAllocation(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.New()
	line := `{"number":%d}` + "\n"

	for i := 0; i < messageCount; i++ {
		store.Add(streammsg.TestMessage(t, strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		// By making this test do some "work" during the processing of a message, we
		// trigger a potential race condition where the actual value of the message
		// is already replaced with a newer message in the channel. This is fixed in
		// this consumer's implementation, but without this test, we couldn't expose
		// the actual problem.
		m := bytes.Split(msg.Value, []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		require.Equal(t, strconv.Itoa(i), string(m[0]))

		i++
	}
}

func BenchmarkConsumer_Messages(b *testing.B) {
	store := inmemstore.New()
	line := `{"number":%d}` + "\n"

	for i := 1; i <= b.N; i++ {
		store.Add(streammsg.TestMessage(b, strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	b.ResetTimer()

	consumer, closer := inmemclient.TestConsumer(b, store)
	defer closer()

	for range consumer.Messages() {
	}
}
