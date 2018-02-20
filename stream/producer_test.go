package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	var _ stream.Producer = (*FakeProducer)(nil)
}

type FakeMessage struct {
	value []byte
}

// Value returns the basic value
func (m *FakeMessage) Value() []byte {
	return m.value
}

// Value returns the basic value
func (m *FakeMessage) SetValue(v []byte) {
	m.value = v
}

func (m *FakeMessage) Ack() error {
	return nil
}

type FakeProducer struct {
	messages chan<- streammsg.Message
}

func (p *FakeProducer) NewMessage(value []byte) streammsg.Message {
	return &FakeMessage{value: value}
}

func (p *FakeProducer) Messages() chan<- streammsg.Message {
	return p.messages
}

func (p *FakeProducer) Close() error {
	return nil
}

func (p FakeProducer) Config() streamconfig.Producer {
	return streamconfig.Producer{}
}
