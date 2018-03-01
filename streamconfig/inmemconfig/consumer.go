package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the inmem client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger *zap.Logger

	// Store is the inmem store from which to consume messages. If left undefined,
	// an internal store will be used.
	Store *inmemstore.Store
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger: zap.NewNop(),
	Store:  inmemstore.New(),
}
