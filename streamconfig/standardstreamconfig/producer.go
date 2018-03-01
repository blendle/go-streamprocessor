package standardstreamconfig

import (
	"io"
	"os"

	"go.uber.org/zap"
)

// Producer is a value-object, containing all user-configurable configuration
// values that dictate how the standard stream client's producer will behave.
type Producer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger *zap.Logger

	// Writer is the object that implements the io.Writer interface. This object
	// is used to write new messages to the stream. Defaults to `os.Stdout`.
	Writer io.Writer
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Logger: zap.NewNop(),
	Writer: os.Stdout,
}
