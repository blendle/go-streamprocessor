package inmemclient_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
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
}

func TestConsumer_Messages(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	_ = store.Add(stream.TestMessage(t, "key1", "hello world"))
	_ = store.Add(stream.TestMessage(t, "key2", "hello universe!"))

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
		_ = store.Add(stream.TestMessage(t, strconv.Itoa(i), "hello world"+strconv.Itoa(i)))
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
		_ = store.Add(stream.TestMessage(t, strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		// By making this test do some "work" during the processing of a message, we
		// trigger a potential race condition where the actual value of the message
		// is already replaced with a newer message in the channel. This is fixed in
		// this consumer's implementation, but without this test, we wouldn't expose
		// the actual problem.
		m := bytes.Split(msg.Value, []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		require.Equal(t, strconv.Itoa(i), string(m[0]))

		i++
	}
}

func TestConsumer_Errors(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Consumer) {
		c.HandleErrors = true
	}

	consumer, closer := inmemclient.TestConsumer(t, nil, options)
	defer closer()

	err := <-consumer.Errors()
	require.Error(t, err)
	assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
}

func TestConsumer_Errors_Manual(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Consumer) {
		c.HandleErrors = false
	}

	consumer, closer := inmemclient.TestConsumer(t, nil, options)
	defer closer()

	select {
	case err := <-consumer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(10 * time.Millisecond):
	}
}

func TestConsumer_Ack(t *testing.T) {
	t.Parallel()

	consumer, closer := inmemclient.TestConsumer(t, nil)
	defer closer()

	assert.Nil(t, consumer.Ack(stream.Message{}))
}

func TestConsumer_Nack(t *testing.T) {
	t.Parallel()

	consumer, closer := inmemclient.TestConsumer(t, nil)
	defer closer()

	assert.Nil(t, consumer.Nack(stream.Message{}))
}

func TestConsumer_Close(t *testing.T) {
	t.Parallel()

	opts := func(c *streamconfig.Consumer) {
		c.Inmem.ConsumeOnce = false
	}

	consumer, err := inmemclient.NewConsumer(opts)
	require.NoError(t, err)

	ch := make(chan error)
	go func() {
		ch <- consumer.Close() // Close is working as expected, and the consumer is terminated.
		ch <- consumer.Close() // Close should return nil immediately, due to `sync.Once`.
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-ch:
			assert.NoError(t, err)
		case <-time.After(testutil.MultipliedDuration(t, 1*time.Second)):
			t.Fatal("timeout while waiting for close to finish")
		}
	}
}

func BenchmarkConsumer_Messages(b *testing.B) {
	store := inmemstore.New()
	line := `{"number":%d}` + "\n"

	for i := 1; i <= b.N; i++ {
		_ = store.Add(stream.TestMessage(b, strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	b.ResetTimer()

	consumer, closer := inmemclient.TestConsumer(b, store)
	defer closer()

	for range consumer.Messages() {
	}
}
