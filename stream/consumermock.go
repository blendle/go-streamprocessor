package stream

import (
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

// ConsumerMock is a mock implementation of the Consumer interface
type ConsumerMock struct {
	Configuration streamconfig.Consumer
	MessagesChan  chan streammsg.Message
}

// Messages implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Messages() <-chan streammsg.Message {
	return c.MessagesChan
}

// Ack implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Ack(_ streammsg.Message) error {
	return nil
}

// Nack implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Nack(_ streammsg.Message) error {
	return nil
}

// Close implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Close() error {
	close(c.MessagesChan)
	return nil
}

// Config implements the Consumer interface for ConsumerMock.
func (c ConsumerMock) Config() streamconfig.Consumer {
	return c.Configuration
}
