package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

// Producer is a value-object, containing all user-configurable configuration
// values that dictate how the inmem client's producer will behave.
type Producer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger

	// Store is the inmem store to which to produce messages. If left undefined,
	// an internal store will be used.
	Store *inmemstore.Store
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Logger: *zap.NewNop(),
	Store:  inmemstore.New(),
}
