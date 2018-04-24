package inmemclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
	"github.com/stretchr/testify/assert"
)

func TestTestConsumer(t *testing.T) {
	t.Parallel()

	consumer, closer := inmemclient.TestConsumer(t, nil)
	defer closer()

	assert.Equal(t, "*inmemclient.Consumer", reflect.TypeOf(consumer).String())
}

func TestTestConsumer_WithStore(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	_, closer := inmemclient.TestConsumer(t, store)
	defer closer()
}

func TestTestConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	options := func(c *streamconfig.Consumer) {
		c.Inmem.Store = store
	}

	// setting the second argument (the store) to nil will instantiate a new store
	// in the initializer, but since we also pass in our own store as an optional
	// argument, it will be the eventual store used by this consumer.
	_, closer := inmemclient.TestConsumer(t, nil, options)
	defer closer()
}

func TestTestProducer(t *testing.T) {
	t.Parallel()

	producer, closer := inmemclient.TestProducer(t, nil)
	defer closer()

	assert.Equal(t, "*inmemclient.Producer", reflect.TypeOf(producer).String())
}

func TestTestProducer_WithStore(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	producer, closer := inmemclient.TestProducer(t, store)
	defer closer()

	assert.EqualValues(t, store, producer.Config().(streamconfig.Producer).Inmem.Store)
}

func TestTestProducer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.New()
	options := func(c *streamconfig.Producer) {
		c.Inmem.Store = store
	}

	// setting the second argument (the store) to nil will instantiate a new store
	// in the initializer, but since we also pass in our own store as an optional
	// argument, it will be the eventual store used by this producer.
	producer, closer := inmemclient.TestProducer(t, nil, options)
	defer closer()

	assert.EqualValues(t, store, producer.Config().(streamconfig.Producer).Inmem.Store)
}
