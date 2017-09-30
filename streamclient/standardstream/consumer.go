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
	var err error

	consumer := &Consumer{messages: make(chan *stream.Message)}
	consumer.fd, err = os.Open(c.ConsumerFD.Name())
	if err != nil {
		panic(err)
	}

	go func(ch chan *stream.Message, fd *os.File) {
		scanner := bufio.NewScanner(fd)
		buf := make([]byte, 0, maxCapacity)
		scanner.Buffer(buf, maxCapacity)

		defer close(ch)
		for scanner.Scan() {
			// scanner.Bytes() does not allocate any new memory for the returned
			// bytes. This means that during the next scan, the value will be updated
			// to reflect the value of the next line.
			//
			// Since we pass this value to the messages channel, we need to allocate
			// a new permanent copy of the value, to prevent a scenario where a
			// consumer of the goroutine reads the value too late, resulting in an
			// unexpected messages being returned.
			//
			b := make([]byte, len(scanner.Bytes()))
			copy(b, scanner.Bytes())

			ch <- &stream.Message{Value: bytes.TrimSpace(b)}
		}
	}(consumer.messages, consumer.fd)

	return consumer
}

// Consumer implements the stream.Consumer interface for standardstream.
type Consumer struct {
	messages chan *stream.Message
	fd       *os.File
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	return c.fd.Close()
}
