package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
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

// Usage returns a byte array with a table-based explanation on how to set the
// configuration values using environment variables. This can be used to explain
// usage details to the user of the application.
func (c Consumer) Usage() []byte {
	return usage(c.Name, c)
}
