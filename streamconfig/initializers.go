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
		Inmem:          &inmemconfig.ClientDefaults,
		Kafka:          &kafkaconfig.ClientDefaults,
		Pubsub:         &pubsubconfig.ClientDefaults,
		Standardstream: &standardstreamconfig.ClientDefaults,
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
		Inmem:          &inmemconfig.ConsumerDefaults,
		Kafka:          &kafkaconfig.ConsumerDefaults,
		Pubsub:         &pubsubconfig.ConsumerDefaults,
		Standardstream: &standardstreamconfig.ConsumerDefaults,
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
		Inmem:          &inmemconfig.ProducerDefaults,
		Kafka:          &kafkaconfig.ProducerDefaults,
		Pubsub:         &pubsubconfig.ProducerDefaults,
		Standardstream: &standardstreamconfig.ProducerDefaults,
	}

	for _, option := range options {
		option(config)
	}

	return config, nil
}
