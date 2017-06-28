package inmem

import "github.com/blendle/go-streamprocessor/stream"

// Client provides access to the streaming capabilities.
type Client struct {
	ConsumerTopic string
	ProducerTopic string

	store *Store
}

// NewClient returns a new inmem client.
func NewClient(options ...func(*Client)) stream.Client {
	store := NewStore()
	return NewClientWithStore(store, options...)
}

// NewClientWithStore returns a new inmem client with the predefined store.
func NewClientWithStore(store *Store, options ...func(*Client)) stream.Client {
	client := &Client{store: store}

	for _, option := range options {
		option(client)
	}

	return client
}

// NewConsumerAndProducer is a convenience method that returns both a consumer
// and a producer, with a single function call.
func (c *Client) NewConsumerAndProducer() (stream.Consumer, stream.Producer) {
	return c.NewConsumer(), c.NewProducer()
}