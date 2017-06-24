package standardstream

import "github.com/blendle/go-streamprocessor/stream"

// Client provides access to the streaming capabilities.
type Client struct{}

// NewClient returns a new standardstream client.
func NewClient() stream.Client {
	return &Client{}
}

// NewConsumerAndProducer is a convenience method that returns both a consumer
// and a producer, with a single function call.
func (c *Client) NewConsumerAndProducer() (stream.Consumer, stream.Producer) {
	return c.NewConsumer(), c.NewProducer()
}
