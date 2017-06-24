package stream

// Consumer interface to be implemented by different streamclients.
type Consumer interface {
	Messages() <-chan *Message
}
