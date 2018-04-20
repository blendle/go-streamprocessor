package inmemclient

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/require"
)

// TestConsumer returns a new inmem consumer to be used in test cases. It also
// returns a function that should be deferred to clean up resources.
//
// You can either pass a pre-configured inmemstore to this function as its
// second argument, or pass in `nil`, to have one be instantiated for you.
//
// You can optionally provide extra options to be used when instantiating the
// consumer.
func TestConsumer(tb testing.TB, s *inmemstore.Store, options ...func(c *streamconfig.Consumer)) (stream.Consumer, func()) {
	tb.Helper()

	if s == nil {
		s = inmemstore.New()
	}

	options = append(options, func(c *streamconfig.Consumer) {
		c.Inmem.Store = s
	})

	consumer, err := NewConsumer(options...)
	require.NoError(tb, err)

	return consumer, func() { require.NoError(tb, consumer.Close()) }
}

// TestProducer returns a new inmem producer to be used in test cases. It also
// returns a function that should be deferred to clean up resources.
//
// You can either pass a pre-configured inmemstore to this function as its
// second argument, or pass in `nil`, to have one be instantiated for you.
func TestProducer(tb testing.TB, s *inmemstore.Store, options ...func(c *streamconfig.Producer)) (stream.Producer, func()) {
	tb.Helper()

	if s == nil {
		s = inmemstore.New()
	}

	options = append(options, func(c *streamconfig.Producer) {
		c.Inmem.Store = s
	})

	producer, err := NewProducer(options...)
	require.NoError(tb, err)

	return producer, func() { require.NoError(tb, producer.Close()) }
}
