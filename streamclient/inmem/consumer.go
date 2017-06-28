package inmem

import (
	"time"

	"github.com/blendle/go-streamprocessor/stream"
)

// NewConsumer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewConsumer() stream.Consumer {
	consumer := &Consumer{messages: make(chan *stream.Message)}

	go func() {
		defer close(consumer.messages)
		for _, msg := range c.store.NewTopic(c.ConsumerTopic).Messages() {
			consumer.messages <- &stream.Message{Value: msg, Timestamp: time.Now()}
		}
	}()

	return consumer
}

// Consumer implements the stream.Consumer interface for standardstream.
type Consumer struct {
	Topic string

	messages chan *stream.Message
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
}
