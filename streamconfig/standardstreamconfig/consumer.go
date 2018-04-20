package standardstreamconfig

import (
	"io"
	"os"

	"go.uber.org/zap"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the standard stream client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger *zap.Logger

	// Reader is the object that implements the io.ReadCloser interface
	// (io.Reader + io.Closer). This object is used to read messages from the
	// stream. Defaults to `os.Stdin`.
	Reader io.ReadCloser
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger: zap.NewNop(),
	Reader: os.Stdin,
}
