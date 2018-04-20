package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
)

// NewConsumer returns a new Consumer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewConsumer(options ...func(*Consumer)) (Consumer, error) {
	defaults := ConsumerDefaults

	config := &defaults
	config.Inmem = inmemconfig.ConsumerDefaults
	config.Kafka = kafkaconfig.ConsumerDefaults
	config.Pubsub = pubsubconfig.ConsumerDefaults
	config.Standardstream = standardstreamconfig.ConsumerDefaults

	// After we've defined the default values, we overwrite them with any provided
	// custom configuration values.
	for _, option := range options {
		if option == nil {
			continue
		}

		option(config)
	}

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}

// NewProducer returns a new Producer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewProducer(options ...func(*Producer)) (Producer, error) {
	defaults := ProducerDefaults

	config := &defaults
	config.Inmem = inmemconfig.ProducerDefaults
	config.Kafka = kafkaconfig.ProducerDefaults
	config.Pubsub = pubsubconfig.ProducerDefaults
	config.Standardstream = standardstreamconfig.ProducerDefaults

	// After we've defined the default values, we overwrite them with any provided
	// custom configuration values.
	for _, option := range options {
		if option == nil {
			continue
		}

		option(config)
	}

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}
