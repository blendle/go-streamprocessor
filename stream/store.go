package stream

// Store interface to be implemented by different stream stores. A stream store
// knows how to store and retrieve `Message's
type Store interface {
	// Add stores a single `stream.Message` in the store.
	Add(Message) error

	// Del removes a single message from the store.
	Del(Message) error

	// Flush empties an entire store.
	Flush() error

	// Messages returns all the messages in the store.
	Messages() []Message
}
