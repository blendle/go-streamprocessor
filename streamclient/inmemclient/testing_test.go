package inmemclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
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
	consumer, closer := inmemclient.TestConsumer(t, store)
	defer closer()

	assert.EqualValues(t, store, consumer.Config().Inmem.Store)
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

	assert.EqualValues(t, store, producer.Config().Inmem.Store)
}
