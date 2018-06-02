package streamconfig

import "go.uber.org/zap"

// Global is a common set of preferences shared between consumers and producers.
type Global struct {
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
	Logger *zap.Logger `ignored:"true"`

	// Name is the name of the current processor. It is currently only used to
	// determine the prefix for environment-variable based configuration values.
	// For example, if Name is set to `MyProcessor`, then all environment
	// variable-based configurations need to start with `MYPROCESSOR_`. If no name
	// is set, then the prefix is "consumer" is used, so you prepend all
	// environment variables with "CONSUMER_.
	Name string `ignored:"true"`

	// Type is the type of the current processor.
	Type string `envconfig:"client_type"`
}

// GlobalDefaults provide a default of global preferences.
var GlobalDefaults = Global{
	HandleErrors:    true,
	HandleInterrupt: true,
	Name:            "",
	AllowEnvironmentBasedConfiguration: true,
}
