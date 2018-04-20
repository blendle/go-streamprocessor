package standardstreamconfig

import (
	"io"
	"os"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the standard stream client's consumer will behave.
type Consumer struct {
	// Reader is the object that implements the io.ReadCloser interface
	// (io.Reader + io.Closer). This object is used to read messages from the
	// stream. Defaults to `os.Stdin`.
	Reader io.ReadCloser
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Reader: os.Stdin,
}
