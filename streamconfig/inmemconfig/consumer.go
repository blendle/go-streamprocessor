package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

// Consumer is a value-type, containing all user-configurable configuration
// values that dictate how a inmem client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, the client's configured logger will be used.
	Logger *zap.Logger

	// Store is the inmem store from which to consume messages. If left undefined,
	// the client's configured store will be used.
	Store *inmemstore.Store
}

// consumerDefaults holds the default values for Consumer.
var consumerDefaults = Consumer{}

// ConsumerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ConsumerDefaults(c Client) Consumer {
	config := consumerDefaults
	config.Logger = c.Logger
	config.Store = c.Store

	return config
}
