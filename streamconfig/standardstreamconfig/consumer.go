package standardstreamconfig

import (
	"io"
	"os"

	"go.uber.org/zap"
)

// Consumer is a value-type, containing all user-configurable configuration
// values that dictate how a standard stream client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, the client's configured logger will be used.
	Logger *zap.Logger

	// Reader is the object that implements the io.ReadCloser interface
	// (io.Reader + io.Closer). This object is used to read messages from the
	// stream. Defaults to `consumerDefaults.Reader`.
	Reader io.ReadCloser
}

// consumerDefaults holds the default values for Consumer.
var consumerDefaults = Consumer{
	Reader: os.Stdin,
}

// ConsumerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ConsumerDefaults(c Client) Consumer {
	config := consumerDefaults
	config.Logger = c.Logger

	return config
}
