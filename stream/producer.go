package stream

// Producer interface to be implemented by different stream clients.
type Producer interface {
	ErrorCloser

	// Messages is a write-only channel on which you can deliver any messages that
	// need to be produced on the message stream.
	//
	// The channel accepts `stream.Message` value objects.
	Messages() chan<- Message

	// Config returns the final configuration used by the producer as an
	// interface. To access the configuration, cast the interface to a
	// `streamconfig.Producer` struct.
	Config() interface{}
}
