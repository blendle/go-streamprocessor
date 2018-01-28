package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
)

// NewClient returns a new Client configuration struct, containing the values
// passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewClient(options ...func(*Client)) (*Client, error) {
	config := &Client{
		Inmem:          &inmemconfig.Client{},
		Kafka:          &kafkaconfig.Client{},
		Pubsub:         &pubsubconfig.Client{},
		Standardstream: &standardstreamconfig.Client{},
	}

	for _, option := range options {
		option(config)
	}

	return config, nil
}

// NewConsumer returns a new Consumer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewConsumer(options ...func(*Consumer)) (*Consumer, error) {
	config := &Consumer{
		Inmem:          &inmemconfig.Consumer{},
		Kafka:          &kafkaconfig.Consumer{},
		Pubsub:         &pubsubconfig.Consumer{},
		Standardstream: &standardstreamconfig.Consumer{},
	}

	for _, option := range options {
		option(config)
	}

	return config, nil
}

// NewProducer returns a new Producer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewProducer(options ...func(*Producer)) (*Producer, error) {
	config := &Producer{
		Inmem:          &inmemconfig.Producer{},
		Kafka:          &kafkaconfig.Producer{},
		Pubsub:         &pubsubconfig.Producer{},
		Standardstream: &standardstreamconfig.Producer{},
	}

	for _, option := range options {
		option(config)
	}

	return config, nil
}
