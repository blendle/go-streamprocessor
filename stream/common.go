package stream

// ErrorCloser interface contains a shared subset of methods between consumers
// and producers. This subset can be used to collectively listen to errors from
// any of the configured stream consumers or producers, and close them all when
// one triggers an error.
type ErrorCloser interface {
	// Errors is a read-only channel on which the consumer or producer delivers
	// any errors that occurred while consuming from, or producing to the stream.
	Errors() <-chan error

	// Close closes the consumer or producer. After calling this method, the
	// consumer or producer is no longer in a usable state, and future method
	// calls can result in panics.
	//
	// Check the specific implementations to know what happens when calling close,
	// but in general any active connection to the message stream is terminated
	// and the messages channel is closed.
	Close() error
}
