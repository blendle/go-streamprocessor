package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
)

type FakeProducer struct {
	messages chan<- *stream.Message
}

func (fp *FakeProducer) Messages() chan<- *stream.Message {
	return fp.messages
}

func (fp *FakeProducer) Close() error {
	return nil
}

func TestProducer(t *testing.T) {
	var _ stream.Producer = (*FakeProducer)(nil)
}
