package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
)

type FakeConsumer struct {
	messages chan *stream.Message
}

func (fc *FakeConsumer) Messages() <-chan *stream.Message {
	return fc.messages
}

func (fc *FakeConsumer) Close() error {
	return nil
}

func (fc *FakeConsumer) Config() streamconfig.Consumer {
	return streamconfig.Consumer{}
}

func TestConsumer(t *testing.T) {
	var _ stream.Consumer = (*FakeConsumer)(nil)
}
