package inmemclient_test

import (
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

func TestNewProducer(t *testing.T) {
	t.Parallel()

	producer, err := inmemclient.NewProducer()
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())

	assert.Equal(t, "*inmemclient.producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	producer, err := inmemclient.NewProducer(streamconfig.InmemStore(inmemstore.New()))
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())
}

func TestProducer_Messages(t *testing.T) {
	t.Parallel()

	expected := "hello world\n"
	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- stream.Message{Value: []byte(expected)}

	waitForMessageCount(t, 1, store.Messages)
	messages := store.Messages()

	require.NotNil(t, messages[0])
	assert.Equal(t, expected, string(messages[0].Value))
}

func TestProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- stream.Message{Value: []byte(strconv.Itoa(i))}
	}

	waitForMessageCount(t, messageCount, store.Messages)

	for i, msg := range store.Messages() {
		require.Equal(t, strconv.Itoa(i), string(msg.Value))
	}
}

func TestProducer_Errors(t *testing.T) {
	t.Parallel()

	options := streamconfig.ProducerOptions(func(c *streamconfig.Producer) {
		c.HandleErrors = true
	})

	producer, closer := inmemclient.TestProducer(t, nil, options)
	defer closer()

	err := <-producer.Errors()
	require.Error(t, err)
	assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
}

func TestProducer_Errors_Manual(t *testing.T) {
	t.Parallel()

	producer, closer := inmemclient.TestProducer(t, nil, streamconfig.ManualErrorHandling())
	defer closer()

	select {
	case err := <-producer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(10 * time.Millisecond):
	}
}

func TestProducer_Close(t *testing.T) {
	t.Parallel()

	producer, err := inmemclient.NewProducer()
	require.NoError(t, err)

	ch := make(chan error)
	go func() {
		ch <- producer.Close() // Close is working as expected, and the producer is terminated.
		ch <- producer.Close() // Close should return nil immediately, due to `sync.Once`.
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

func TestProducer_Close_WithoutInterrupt(t *testing.T) {
	t.Parallel()

	producer, err := inmemclient.NewProducer(streamconfig.ManualInterruptHandling())
	require.NoError(t, err)

	ch := make(chan error)
	go func() {
		ch <- producer.Close() // Close is working as expected, and the producer is terminated.
		ch <- producer.Close() // Close should return nil immediately, due to `sync.Once`.
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

func BenchmarkProducer_Messages(b *testing.B) {
	producer, closer := inmemclient.TestProducer(b, nil)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- stream.Message{Value: []byte(fmt.Sprintf(`{"number":%d}`, i))}
	}
}

// waitForMessageCount accepts a count, and a function that returns an array of
// `stream.Message`s. It will try 100 times to call the provided function and
// match the number of the returned messages against the provided count. If a
// match is found anywhere between the first and last run, the function returns,
// if no match is ever found, the test calling this function will fail.
func waitForMessageCount(tb testing.TB, count int, f func() []stream.Message) {
	tb.Helper()

	var messages []stream.Message
	for i := 0; i < 100; i++ {
		messages = f()
		if len(messages) >= count {
			return
		}

		time.Sleep(1 * time.Millisecond)
	}

	require.Len(tb, messages, count, "unexpected amount of messages counted")
}
