package inmemclient_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
	"github.com/blendle/go-streamprocessor/v3/streamstore/inmemstore"
	"github.com/blendle/go-streamprocessor/v3/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	consumer, err := inmemclient.NewConsumer()
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	assert.Equal(t, "*inmemclient.consumer", reflect.TypeOf(consumer).String())
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	consumer, err := inmemclient.NewConsumer(streamconfig.InmemStore(inmemstore.New()))
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

	messageCount := 10000
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

	messageCount := 10000
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

func TestConsumer_Backlog(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	_ = store.Add(stream.Message{})
	_ = store.Add(stream.Message{})

	// We pass the `InmemListen` option, which makes this consumer behave more
	// like a regular consumer, continuously listening for new messages. Old
	// messages are discarded once read, which allows us to track the number of
	// messages that still need to be consumed.
	consumer, closer := inmemclient.TestConsumer(t, store, streamconfig.InmemListen())
	defer closer()

	time.Sleep(100 * time.Millisecond)

	for _, want := range []int{2, 1, 0} {
		got, err := consumer.Backlog()
		require.NoError(t, err)
		assert.Equal(t, want, got)

		if want > 0 {
			<-consumer.Messages()
			time.Sleep(time.Millisecond) // wait for the message to be deleted
		}
	}
}

func TestConsumer_Backlog_ConsumeOnce(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	_ = store.Add(stream.Message{})
	_ = store.Add(stream.Message{})

	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	for i := 0; i < 3; i++ {
		got, err := consumer.Backlog()
		require.NoError(t, err)

		// When `ConsumeOnce` is enabled for the inmem consumer, there's no notion
		// of an offset, so `0` is always returned.
		assert.Equal(t, 0, got)

		<-consumer.Messages()
	}
}

func TestConsumer_Errors(t *testing.T) {
	t.Parallel()

	options := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.HandleErrors = true
	})

	consumer, closer := inmemclient.TestConsumer(t, nil, options)
	defer closer()

	err := <-consumer.Errors()
	require.Error(t, err)
	assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
}

func TestConsumer_Errors_Manual(t *testing.T) {
	t.Parallel()

	consumer, closer := inmemclient.TestConsumer(t, nil, streamconfig.ManualErrorHandling())
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

	consumer, err := inmemclient.NewConsumer(streamconfig.InmemListen())
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
		case <-time.After(testutil.MultipliedDuration(t, 3*time.Second)):
			t.Fatal("timeout while waiting for close to finish")
		}
	}
}

func TestConsumer_Close_WithoutInterrupt(t *testing.T) {
	t.Parallel()

	consumer, err := inmemclient.NewConsumer(
		streamconfig.InmemListen(),
		streamconfig.ManualInterruptHandling(),
	)
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
		case <-time.After(testutil.MultipliedDuration(t, 3*time.Second)):
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
