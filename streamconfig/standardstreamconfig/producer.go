package standardstreamconfig

import (
	"io"
	"os"

	"go.uber.org/zap"
)

// Producer is a value-type, containing all user-configurable configuration
// values that dictate how a standard stream client's producer will behave.
type Producer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, the client's configured logger will be used.
	Logger *zap.Logger

	// Writer is the object that implements the io.Writer interface. This object
	// is used to write new messages to the stream. Defaults to
	// `producerDefaults.Writer`.
	Writer io.Writer
}

// producerDefaults holds the default values for Producer.
var producerDefaults = Producer{
	Writer: os.Stdout,
}

// ProducerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ProducerDefaults(c Client) Producer {
	config := producerDefaults
	config.Logger = c.Logger

	return config
}
