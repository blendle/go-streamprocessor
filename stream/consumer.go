package stream

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	// Messages is a read-only channel on which the consumer delivers any messages
	// being read from the stream.
	//
	// The channel returns each message as a `stream.Message` value object.
	Messages() <-chan Message

	// Errors is a read-only channel on which the consumer delivers any errors
	// that occurred while consuming from the stream.
	Errors() <-chan error

	// Ack can be used to acknowledge that a message was processed and should not
	// be delivered again.
	Ack(Message) error

	// Nack is the opposite of `Ack`. It can be used to indicate that a message
	// was _not_ processed, and should be delivered again in the future.
	Nack(Message) error

	// Close closes the consumer. After calling this method, the consumer is no
	// longer in a usable state, and subsequent method calls can result in
	// panics.
	//
	// Check the specific implementations to know what exactly happens when
	// calling close, but in general any active connection to the message stream
	// is terminated and the messages channel is closed.
	Close() error

	// Config returns the final configuration used by the consumer as an
	// interface. To access the configuration, cast the interface to a
	// `streamconfig.Consumer` struct.
	Config() interface{}
}
