package stream

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	Messages() <-chan *Message
	Close() error
}
