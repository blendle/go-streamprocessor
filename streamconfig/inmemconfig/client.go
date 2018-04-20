package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

// Client is a value-type, containing all user-configurable configuration values
// that dictate how the instantiated inmem client will behave.
type Client struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger *zap.Logger

	// Store is the inmem store to which to publish, and from which to receive
	// messages. This store can be overwritten for either the consumer or the
	// producer.
	Store *inmemstore.Store
}

// clientDefaults holds the default values for Client.
var clientDefaults = Client{
	Logger: zap.NewNop(),
	Store:  inmemstore.New(),
}

// ClientDefaults returns the provided defaults.
func ClientDefaults() Client {
	return clientDefaults
}
