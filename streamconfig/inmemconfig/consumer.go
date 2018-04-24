package inmemconfig

import (
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the inmem client's consumer will behave.
type Consumer struct {
	// Store is the inmem store from which to consume messages. If left undefined,
	// an internal store will be used.
	Store stream.Store `ignored:"true"`

	// ConsumeOnce dictates whether the inmem consumer should request all messages
	// in the configured `inmemstore` once, or if it should keep listening for any
	// new messages being added to the store. This toggle is useful for different
	// test-cases where you either want to fetch all messages once and continue
	// the test, or you want to start the consumer in a separate goroutine in the
	// test-setup and add messages at different intervals _after_ you started the
	// consumer. Note that there's a major side-effect right now to setting
	// `ConsumeOnce` to `false`, which is that the consumer will remove any
	// consumed messages from the configured `inmemstore` to prevent consuming
	// duplicate messages. This behavior is not the case when `ConsumeOnce` is
	// kept at the default `true`, meaning messages will stay in the store, even
	// when consumed.
	//
	// Defaults to `true`, meaning the consumer will auto-close after consuming
	// all existing messages in the `inmemstore`.
	ConsumeOnce bool
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Store:       inmemstore.New(),
	ConsumeOnce: true,
}
