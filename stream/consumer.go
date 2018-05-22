package stream

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	ErrorCloser

	// Messages is a read-only channel on which the consumer delivers any messages
	// being read from the stream.
	//
	// The channel returns each message as a `stream.Message` value object.
	Messages() <-chan Message

	// Ack can be used to acknowledge that a message was processed and should not
	// be delivered again.
	Ack(Message) error

	// Nack is the opposite of `Ack`. It can be used to indicate that a message
	// was _not_ processed, and should be delivered again in the future.
	Nack(Message) error

	// Config returns the final configuration used by the consumer as an
	// interface. To access the configuration, cast the interface to a
	// `streamconfig.Consumer` struct.
	Config() interface{}
}
