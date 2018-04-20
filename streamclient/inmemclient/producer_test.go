package inmemclient_test

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Producer{}
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	producer, err := inmemclient.NewProducer()
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())

	assert.Equal(t, "*inmemclient.Producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	options := func(c *streamconfig.Producer) {
		c.Inmem.Store = store
	}

	producer, err := inmemclient.NewProducer(options)
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())

	assert.EqualValues(t, store, producer.Config().Inmem.Store)
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	expected := "hello world\n"
	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- streammsg.Message{Value: []byte(expected)}

	waitForMessageCount(t, 1, store.Messages)
	messages := store.Messages()

	require.NotNil(t, messages[0])
	assert.Equal(t, expected, string(messages[0].Value))
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- streammsg.Message{Value: []byte(strconv.Itoa(i))}
	}

	waitForMessageCount(t, messageCount, store.Messages)

	for i, msg := range store.Messages() {
		require.Equal(t, strconv.Itoa(i), string(msg.Value))
	}
}

func BenchmarkProducer_Messages(b *testing.B) {
	producer, closer := inmemclient.TestProducer(b, nil)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- streammsg.Message{Value: []byte(fmt.Sprintf(`{"number":%d}`, i))}
	}
}

func waitForMessageCount(tb testing.TB, count int, f func() []streammsg.Message) {
	tb.Helper()

	var messages []streammsg.Message
	for i := 0; i < 100; i++ {
		messages = f()
		if len(messages) >= count {
			return
		}

		time.Sleep(1 * time.Millisecond)
	}

	require.Len(tb, messages, count)
}
