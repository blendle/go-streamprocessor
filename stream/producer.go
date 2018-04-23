package stream

// Producer interface to be implemented by different stream clients.
type Producer interface {
	// Messages is a write-only channel on which you can deliver any messages that
	// need to be produced on the message stream.
	//
	// The channel accepts `stream.Message` value objects.
	Messages() chan<- Message

	// Errors is a read-only channel on which the producer delivers any errors
	// that occurred while producing to the stream.
	Errors() <-chan error

	// Close closes the producer. After calling this method, the producer is no
	// longer in a usable state, and subsequent method calls can result in
	// panics.
	//
	// Check the specific implementations to know what exactly happens when
	// calling close, but in general no new messages will be delivered to the
	// message stream and the messages channel is closed.
	Close() error

	// Config returns the final configuration used by the producer as an
	// interface. To access the configuration, cast the interface to a
	// `streamconfig.Producer` struct.
	Config() interface{}
}
