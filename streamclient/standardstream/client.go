package standardstream

import (
	"io"
	"os"

	"github.com/blendle/go-streamprocessor/stream"
)

// Client provides access to the streaming capabilities.
type Client struct {
	// ConsumerFD is the file descriptor to consume messages from. If undefined,
	// the `os.Stdin` descriptor will be used.
	ConsumerFD *os.File

	// ProducerFD is the file descriptor to produce messages to. If undefined, the
	// `os.Stdout` descriptor will be used.
	ProducerFD io.Writer
}

// NewClient returns a new standardstream client.
func NewClient(options ...func(*Client)) stream.Client {
	client := &Client{}

	for _, option := range options {
		option(client)
	}

	if client.ConsumerFD == nil {
		client.ConsumerFD = os.Stdin
	}

	if client.ProducerFD == nil {
		client.ProducerFD = os.Stdout
	}

	return client
}

// NewConsumerAndProducer is a convenience method that returns both a consumer
// and a producer, with a single function call.
func (c *Client) NewConsumerAndProducer() (stream.Consumer, stream.Producer) {
	return c.NewConsumer(), c.NewProducer()
}
