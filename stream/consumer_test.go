package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

type FakeConsumer struct {
	messages chan streammsg.Message
}

func (fc *FakeConsumer) Messages() <-chan streammsg.Message {
	return fc.messages
}

func (fc *FakeConsumer) Close() error {
	return nil
}

func (fc *FakeConsumer) Config() streamconfig.Consumer {
	return streamconfig.Consumer{}
}

func TestConsumer(t *testing.T) {
	t.Parallel()

	var _ stream.Consumer = (*FakeConsumer)(nil)
}
