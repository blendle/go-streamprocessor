package stream

// Producer interface to be implemented by different stream clients.
type Producer interface {
	Messages() chan<- *Message
	Close() error
}
