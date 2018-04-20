package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
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

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger

	// HandleInterrupt determines whether the consumer should close itself
	// gracefully when an interrupt signal (^C) is received. This defaults to true
	// to increase first-time ease-of-use, but if the application wants to handle
	// these signals manually, this flag disables the automated implementation.
	HandleInterrupt bool
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

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger

	// HandleInterrupt determines whether the producer should close itself
	// gracefully when an interrupt signal (^C) is received. This defaults to true
	// to increase first-time ease-of-use, but if the application wants to handle
	// these signals manually, this flag disables the automated implementation.
	HandleInterrupt bool
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger:          *zap.NewNop(),
	HandleInterrupt: true,
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Logger:          *zap.NewNop(),
	HandleInterrupt: true,
}
