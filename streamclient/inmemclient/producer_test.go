package inmemclient_test

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
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

	client, err := inmemclient.New()
	require.NoError(t, err)

	producer, err := client.NewProducer()
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())

	assert.Equal(t, "*inmemclient.Producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()

	client, err := inmemclient.New()
	require.NoError(t, err)

	options := func(c *streamconfig.Producer) {
		c.Inmem.Store = store
	}

	producer, err := client.NewProducer(options)
	require.NoError(t, err)
	defer require.NoError(t, producer.Close())

	assert.EqualValues(t, store, producer.Config().Inmem.Store)
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()
	expected := "hello world\n"

	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	producer.Messages() <- producer.NewMessage([]byte(expected))

	require.NotEqual(t, store.Messages(), 0, "expected 1 message, got %d", len(store.Messages()))
	assert.Equal(t, expected, string(store.Messages()[0].Value()))
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.NewStore()

	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- producer.NewMessage([]byte(strconv.Itoa(i)))
	}

	for i, msg := range store.Messages() {
		require.Equal(t, strconv.Itoa(i), string(msg.Value()))
	}
}

func BenchmarkProducer_Messages(b *testing.B) {
	producer, closer := inmemclient.TestProducer(b, nil)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- producer.NewMessage([]byte(fmt.Sprintf(`{"number":%d}`, i)))
	}
}
