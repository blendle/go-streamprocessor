package inmemclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
	"github.com/blendle/go-streamprocessor/v3/streamstore/inmemstore"
	"github.com/stretchr/testify/assert"
)

func TestTestConsumer(t *testing.T) {
	t.Parallel()

	consumer, closer := inmemclient.TestConsumer(t, nil)
	defer closer()

	assert.Equal(t, "*inmemclient.consumer", reflect.TypeOf(consumer).String())
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

	// setting the second argument (the store) to nil will instantiate a new store
	// in the initializer, but since we also pass in our own store as an optional
	// argument, it will be the eventual store used by this consumer.
	consumer, closer := inmemclient.TestConsumer(t, nil, streamconfig.InmemStore(store))
	defer closer()

	assert.EqualValues(t, store, consumer.Config().(streamconfig.Consumer).Inmem.Store)
}

func TestTestProducer(t *testing.T) {
	t.Parallel()

	producer, closer := inmemclient.TestProducer(t, nil)
	defer closer()

	assert.Equal(t, "*inmemclient.producer", reflect.TypeOf(producer).String())
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

	// setting the second argument (the store) to nil will instantiate a new store
	// in the initializer, but since we also pass in our own store as an optional
	// argument, it will be the eventual store used by this producer.
	producer, closer := inmemclient.TestProducer(t, nil, streamconfig.InmemStore(store))
	defer closer()

	assert.EqualValues(t, store, producer.Config().(streamconfig.Producer).Inmem.Store)
}
