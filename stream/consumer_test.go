package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	var _ stream.Consumer = (*stream.ConsumerMock)(nil)
}
