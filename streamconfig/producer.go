package streamconfig

import (
	"strings"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

// Producer contains the configuration for all the different consumer that
// implement the stream.Producer interface. When a producer is instantiated,
// these options can be passed into the new producer to determine its behavior.
// If the producer only has to support a single implementation of the interface,
// then all other configuration values can be ignored.
type Producer struct {
	Inmem          inmemconfig.Producer
	Kafka          kafkaconfig.Producer
	Pubsub         pubsubconfig.Producer
	Standardstream standardstreamconfig.Producer

	Global
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{Global: GlobalDefaults}

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
	*config = config.WithOptions(options...)

	// Finally, we set/overwrite any value with any custom configuration values
	// provided via environment variables. If `AllowEnvironmentBasedConfiguration`
	// is set to false, this step is skipped.
	if config.AllowEnvironmentBasedConfiguration {
		env := "producer"
		if config.Name != "" {
			env = strings.Join([]string{config.Name, env}, "_")
		}

		err := envconfig.Process(env, config)
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
		"Finished preparing producer configuration.",
		zap.Any("config", config),
	)

	// We pass the config by-value, to prevent any race-condition where the
	// original config struct is modified after the fact.
	return *config, nil
}

// WithOptions takes the current Producer, applies the supplied Options, and
// returns the resulting Producer.
func (p Producer) WithOptions(opts ...Option) Producer {
	pp := &p

	for _, opt := range opts {
		if opt == nil {
			continue
		}

		opt.apply(nil, pp)
	}

	return *pp
}
