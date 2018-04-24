package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

// NewConsumer returns a new Consumer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewConsumer(options ...Option) (Consumer, error) {
	defaults := ConsumerDefaults

	config := &defaults
	config.Inmem = inmemconfig.ConsumerDefaults
	config.Kafka = kafkaconfig.ConsumerDefaults
	config.Pubsub = pubsubconfig.ConsumerDefaults
	config.Standardstream = standardstreamconfig.ConsumerDefaults

	// After we've defined the default values, we overwrite any currently defined
	// value with any custom configuration values passed into the `NewConsumer`
	// function.
	for _, option := range options {
		option.apply(config, nil)
	}

	// Finally, we set/overwrite any value with any custom configuration values
	// provided via environment variables. If `AllowEnvironmentBasedConfiguration`
	// is set to false, this step is skipped.
	if config.AllowEnvironmentBasedConfiguration {
		err := envconfig.Process(config.Name, config)
		if err != nil {
			return *config, err
		}
	}

	// Make sure we set a logger if it was explicitly set to nil.
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}

	config.Logger.Info(
		"Finished preparing consumer configuration.",
		zap.Any("config", config),
	)

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}

// NewProducer returns a new Producer configuration struct, containing the
// values passed into the function. If any error occurs during configuration
// validation, an error is returned as the second argument.
func NewProducer(options ...Option) (Producer, error) {
	defaults := ProducerDefaults

	config := &defaults
	config.Inmem = inmemconfig.ProducerDefaults
	config.Kafka = kafkaconfig.ProducerDefaults
	config.Pubsub = pubsubconfig.ProducerDefaults
	config.Standardstream = standardstreamconfig.ProducerDefaults

	// After we've defined the default values, we overwrite any currently defined
	// value with any custom configuration values passed into the `NewProducer`
	// function.
	for _, option := range options {
		option.apply(nil, config)
	}

	// Finally, we set/overwrite any value with any custom configuration values
	// provided via environment variables. If `AllowEnvironmentBasedConfiguration`
	// is set to false, this step is skipped.
	if config.AllowEnvironmentBasedConfiguration {
		err := envconfig.Process(config.Name, config)
		if err != nil {
			return *config, err
		}
	}

	// Make sure we set a logger if it was explicitly set to nil.
	if config.Logger == nil {
		config.Logger = zap.NewNop()
	}

	config.Logger.Info(
		"Finished preparing producer configuration.",
		zap.Any("config", config),
	)

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}
