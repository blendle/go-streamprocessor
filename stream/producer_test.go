package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	var _ stream.Producer = (*stream.ProducerMock)(nil)
}
