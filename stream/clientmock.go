package stream

import "github.com/blendle/go-streamprocessor/streamconfig"

// ClientMock is a mock implementation of the Client interface
type ClientMock struct {
	Configuration streamconfig.Client
}

// NewConsumer implements the Client interface for ClientMock.
func (c *ClientMock) NewConsumer(options ...func(*streamconfig.Consumer)) (Consumer, error) {
	config, err := streamconfig.NewConsumer(streamconfig.Client{}, options...)
	if err != nil {
		return nil, err
	}

	return &ConsumerMock{Configuration: config}, nil
}

// NewProducer implements the Client interface for ClientMock.
func (c *ClientMock) NewProducer(options ...func(*streamconfig.Producer)) (Producer, error) {
	config, err := streamconfig.NewProducer(streamconfig.Client{}, options...)
	if err != nil {
		return nil, err
	}

	return &ProducerMock{Configuration: config}, nil
}

// Config implements the Client interface for ClientMock.
func (c ClientMock) Config() streamconfig.Client {
	return c.Configuration
}
