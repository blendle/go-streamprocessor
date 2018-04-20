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
}

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
}
