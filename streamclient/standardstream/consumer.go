package standardstream

import (
	"bufio"
	"bytes"
	"os"

	"github.com/blendle/go-streamprocessor/stream"
)

const maxCapacity = 512 * 1024

// NewConsumer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewConsumer() stream.Consumer {
	var b []byte
	consumer := &Consumer{messages: make(chan *stream.Message)}

	f, _ := os.Open(c.ConsumerFD.Name())

	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	go func() {
		defer close(consumer.messages)
		for scanner.Scan() {
			b = scanner.Bytes()
			consumer.messages <- &stream.Message{Value: bytes.TrimSpace(b)}
		}
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

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	return nil
}
