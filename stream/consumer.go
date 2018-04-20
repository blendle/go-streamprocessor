package stream

import (
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	// Messages is a read-only channel on which the consumer delivers any messages
	// being read from the stream.
	//
	// The channel returns each message as a `streammsg.Message` value object.
	Messages() <-chan streammsg.Message

	// Ack can be used to acknowledge that a message was processed and should not
	// be delivered again.
	Ack(streammsg.Message) error

	// Nack is the opposite of `Ack`. It can be used to indicate that a message
	// was _not_ processed, and should be delivered again in the future.
	Nack(streammsg.Message) error

	// Close closes the consumer. After calling this method, the consumer is no
	// longer in a usable state, and subsequent method calls can result in
	// panics.
	//
	// Check the specific implementations to know what exactly happens when
	// calling close, but in general any active connection to the message stream
	// is terminated and the messages channel is closed.
	Close() error

	// Config returns the final configuration used by the consumer.
	Config() streamconfig.Consumer
}
