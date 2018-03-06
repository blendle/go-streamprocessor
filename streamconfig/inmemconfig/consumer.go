package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the inmem client's consumer will behave.
type Consumer struct {
	// Store is the inmem store from which to consume messages. If left undefined,
	// an internal store will be used.
	Store *inmemstore.Store `ignored:"true"`
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Store: inmemstore.New(),
}
