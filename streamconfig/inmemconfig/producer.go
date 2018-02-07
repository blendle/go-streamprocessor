package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

// Producer is a value-type, containing all user-configurable configuration
// values that dictate how a inmem client's producer will behave.
type Producer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, the client's configured logger will be used.
	Logger *zap.Logger

	// Store is the inmem store to which to produce messages. If left undefined,
	// the client's configured store will be used.
	Store *inmemstore.Store
}

// producerDefaults holds the default values for Producer.
var producerDefaults = Producer{}

// ProducerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ProducerDefaults(c Client) Producer {
	config := producerDefaults
	config.Logger = c.Logger
	config.Store = c.Store

	return config
}
