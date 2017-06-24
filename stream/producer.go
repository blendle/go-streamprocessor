package stream

// Producer interface to be implemented by different streamclients.
type Producer interface {
	Messages() chan<- *Message
	Close() error
	PartitionKey(func(msg *Message) []byte)
}
