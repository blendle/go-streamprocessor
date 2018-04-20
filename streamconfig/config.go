package streamconfig

import (
	"bytes"
	"text/tabwriter"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

// Consumer contains the configuration for all the different consumer that
// implement the stream.Consumer interface. When a consumer is instantiated,
// these options can be passed into the new consumer to determine its behavior.
// If the consumer only has to support a single implementation of the interface,
// then all other configuration values can be ignored.
type Consumer struct { // nolint:malign
	Inmem          inmemconfig.Consumer
	Kafka          kafkaconfig.Consumer
	Pubsub         pubsubconfig.Consumer
	Standardstream standardstreamconfig.Consumer

	// AllowEnvironmentBasedConfiguration allows you to disable configuring the
	// stream client based on predefined environment variables. This is enabled by
	// default, but can be disabled if you want full control over the behavior of
	// the stream client without any outside influence.
	AllowEnvironmentBasedConfiguration bool `ignored:"true"`

	// HandleErrors determines whether the consumer should handle any stream
	// errors by itself, and terminate the application if any error occurs. This
	// defaults to true. If manually set to false, the `Errors()` channel needs to
	// be consumed manually, and any appropriate action needs to be taken when an
	// errors occurs, otherwise the consumer will get stuck once an error occurs.
	HandleErrors bool `ignored:"true"`

	// HandleInterrupt determines whether the consumer should close itself
	// gracefully when an interrupt signal (^C) is received. This defaults to true
	// to increase first-time ease-of-use, but if the application wants to handle
	// these signals manually, this flag disables the automated implementation.
	HandleInterrupt bool `ignored:"true"`

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger `ignored:"true"`

	// Name is the name of the current processor. It is currently only used to
	// determine the prefix for environment-variable based configuration values.
	// For example, if Name is set to `MyProcessor`, then all environment
	// variable-based configurations need to start with `MYPROCESSOR_`. If no name
	// is set, then the prefix is "consumer" is used, so you prepend all
	// environment variables with "CONSUMER_.
	Name string `ignored:"true"`
}

// Producer contains the configuration for all the different consumer that
// implement the stream.Producer interface. When a producer is instantiated,
// these options can be passed into the new producer to determine its behavior.
// If the producer only has to support a single implementation of the interface,
// then all other configuration values can be ignored.
type Producer struct { // nolint:malign
	Inmem          inmemconfig.Producer
	Kafka          kafkaconfig.Producer
	Pubsub         pubsubconfig.Producer
	Standardstream standardstreamconfig.Producer

	// AllowEnvironmentBasedConfiguration allows you to disable configuring the
	// stream client based on predefined environment variables. This is enabled by
	// default, but can be disabled if you want full control over the behavior of
	// the stream client without any outside influence.
	AllowEnvironmentBasedConfiguration bool `ignored:"true"`

	// HandleErrors determines whether the consumer should handle any stream
	// errors by itself, and terminate the application if any error occurs. This
	// defaults to true. If manually set to false, the `Errors()` channel needs to
	// be consumed manually, and any appropriate action needs to be taken when an
	// errors occurs, otherwise the consumer will get stuck once an error occurs.
	HandleErrors bool `ignored:"true"`

	// HandleInterrupt determines whether the producer should close itself
	// gracefully when an interrupt signal (^C) is received. This defaults to true
	// to increase first-time ease-of-use, but if the application wants to handle
	// these signals manually, this flag disables the automated implementation.
	HandleInterrupt bool `ignored:"true"`

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger `ignored:"true"`

	// Name is the name of the current processor. It is currently only used to
	// determine the prefix for environment-variable based configuration values.
	// For example, if Name is set to `MyProcessor`, then all environment
	// variable-based configurations need to start with `MYPROCESSOR_`.If no name
	// is set, then the prefix is "producer" is used, so you prepend all
	// environment variables with "PRODUCER_.
	Name string `ignored:"true"`
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger:          *zap.NewNop(),
	HandleErrors:    true,
	HandleInterrupt: true,
	Name:            "consumer",
	AllowEnvironmentBasedConfiguration: true,
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Logger:          *zap.NewNop(),
	HandleErrors:    true,
	HandleInterrupt: true,
	Name:            "producer",
	AllowEnvironmentBasedConfiguration: true,
}

// Usage calls the usage() function and returns a byte array.
// Usage returns a byte array with a table-based explanation on how to set the
// configuration values using environment variables. This can be used to explain
// usage details to the user of the application.
func (c Consumer) Usage() []byte {
	return usage(c.Name, c)
}

// Usage returns a byte array with a table-based explanation on how to set the
// configuration values using environment variables. This can be used to explain
// usage details to the user of the application.
func (p Producer) Usage() []byte {
	return usage(p.Name, p)
}

func usage(name string, inf interface{}) []byte {
	var err error
	b := &bytes.Buffer{}
	tabs := tabwriter.NewWriter(b, 1, 0, 4, ' ', 0)

	if c, ok := inf.(Consumer); ok {
		err = envconfig.Usagef(name, &c, tabs, envconfig.DefaultTableFormat)
	}

	if p, ok := inf.(Producer); ok {
		err = envconfig.Usagef(name, &p, tabs, envconfig.DefaultTableFormat)
	}

	if err != nil {
		return nil
	}

	if tabs.Flush() != nil {
		return nil
	}

	return b.Bytes()
}
