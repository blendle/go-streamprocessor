package standardstreamclient

import (
	"bytes"
	"io"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/stretchr/testify/require"
)

// TestBuffer returns an io.ReadWriter that can be used during testing. You can
// pass additional string arguments after the first argument. Each extra
// argument is written as a line to the buffer, delimited by \n.
func TestBuffer(tb testing.TB, v ...string) io.ReadWriteCloser {
	b := &testBuffer{}

	for _, s := range v {
		_, err := b.WriteString(s + "\n")
		require.NoError(tb, err)
	}

	return b
}

// TestConsumer returns a new standardstream consumer to be used in test cases.
// You pass in an io.ReadCloser object as the second argument.
//
// The return value is the consumer, and a function that should be deferred to
// clean up resources.
func TestConsumer(tb testing.TB, r io.ReadCloser) (stream.Consumer, func()) {
	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = r
	}

	consumer, err := NewConsumer(options)
	require.NoError(tb, err)

	return consumer, func() { require.NoError(tb, consumer.Close()) }
}

// TestProducer returns a new standardstream producer to be used in test cases.
// You pass in an io.Writer object as the second argument.
//
// The return value is the producer, and a function that should be deferred to
// clean up resources.
func TestProducer(tb testing.TB, w io.Writer) (stream.Producer, func()) {
	options := func(c *streamconfig.Producer) {
		c.Standardstream.Writer = w
	}

	producer, err := NewProducer(options)
	require.NoError(tb, err)

	return producer, func() { require.NoError(tb, producer.Close()) }
}

// testBuffer is just here to make bytes.Buffer an io.ReadWriteCloser.
type testBuffer struct {
	bytes.Buffer
}

// Close adds a Close method to our buffer so we satisfy io.ReadWriteCloser.
func (b *testBuffer) Close() error {
	return nil
}
