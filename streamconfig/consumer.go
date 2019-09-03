package streamconfig

import (
	"strings"

	"github.com/blendle/go-streamprocessor/v3/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/standardstreamconfig"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

// Consumer contains the configuration for all the different consumer that
// implement the stream.Consumer interface. When a consumer is instantiated,
// these options can be passed into the new consumer to determine its behavior.
// If the consumer only has to support a single implementation of the interface,
// then all other configuration values can be ignored.
type Consumer struct {
	Inmem          inmemconfig.Consumer
	Kafka          kafkaconfig.Consumer
	Pubsub         pubsubconfig.Consumer
	Standardstream standardstreamconfig.Consumer

	Global
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{Global: GlobalDefaults}

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
	*config = config.WithOptions(options...)

	// Finally, we set/overwrite any value with any custom configuration values
	// provided via environment variables. If `AllowEnvironmentBasedConfiguration`
	// is set to false, this step is skipped.
	if config.AllowEnvironmentBasedConfiguration {
		var err error

		*config, err = config.FromEnv()
		if err != nil {
			return *config, err
		}
	}

	// Make sure we set a logger if it was explicitly set to nil.
	if config.Logger == nil {
		cfg := zap.NewProductionConfig()
		cfg.Level.SetLevel(zap.ErrorLevel)

		log, err := cfg.Build()
		if err != nil {
			return *config, err
		}

		config.Logger = log
	}

	config.Logger.Info(
		"Finished preparing consumer configuration.",
		zap.Any("config", config),
	)

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}

// WithOptions takes the current Consumer, applies the supplied Options, and
// returns the resulting Consumer.
func (c Consumer) WithOptions(opts ...Option) Consumer {
	cc := &c

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt.apply(cc, nil)
	}

	return *cc
}

// FromEnv populates the Consumer based on the environment variables set with
// the prefix based on the consumer name.
func (c Consumer) FromEnv() (Consumer, error) {
	cc := &c

	env := "consumer"
	if c.Name != "" {
		env = strings.Join([]string{c.Name, env}, "_")
	}

	return *cc, envconfig.Process(env, cc)
}
