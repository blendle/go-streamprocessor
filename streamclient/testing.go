package streamclient

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamutil/testutils"
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
	case <-time.After(testutils.MultipliedDuration(tb, 3*time.Second)):
		require.Fail(tb, "Timeout while waiting for message to be returned.")
	}

	return stream.Message{}
}
