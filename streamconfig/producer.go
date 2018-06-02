package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
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

// Usage returns a byte array with a table-based explanation on how to set the
// configuration values using environment variables. This can be used to explain
// usage details to the user of the application.
func (p Producer) Usage() []byte {
	return usage(p.Name, p)
}
