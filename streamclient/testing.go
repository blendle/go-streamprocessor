package streamclient

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamutil/testutil"
	"github.com/stretchr/testify/require"
)

// TestMessageFromConsumer returns a single message, consumed from the provided
// consumer. It has a built-in timeout mechanism to prevent the test from
// getting stuck.
func TestMessageFromConsumer(tb testing.TB, consumer stream.Consumer) stream.Message {
	tb.Helper()

	select {
	case m := <-consumer.Messages():
		require.NotNil(tb, m)

		return m
	case <-time.After(testutil.MultipliedDuration(tb, 10*time.Second)):
		require.Fail(tb, "Timeout while waiting for message to be returned.")
	}

	return stream.Message{}
}
