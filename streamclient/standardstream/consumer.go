package standardstream

import (
	"bufio"
	"os"

	"github.com/blendle/go-streamprocessor/stream"
)

// NewConsumer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewConsumer() stream.Consumer {
	consumer := &Consumer{messages: make(chan *stream.Message)}

	scanner := bufio.NewScanner(os.Stdin)
	go func() {
		for scanner.Scan() {
			consumer.messages <- &stream.Message{Value: scanner.Bytes()}
		}
		close(consumer.messages)
	}()

	return consumer
}

// Consumer implements the stream.Consumer interface for standardstream.
type Consumer struct {
	messages chan *stream.Message
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
}
