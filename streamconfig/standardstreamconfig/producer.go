package standardstreamconfig

import (
	"io"
	"os"
)

// Producer is a value-object, containing all user-configurable configuration
// values that dictate how the standard stream client's producer will behave.
type Producer struct {
	// Writer is the object that implements the io.Writer interface. This object
	// is used to write new messages to the stream. Defaults to `os.Stdout`.
	Writer io.Writer
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Writer: os.Stdout,
}
