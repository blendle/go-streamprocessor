package stream

// Client interface to be implemented by different stream clients.
type Client interface {
	NewConsumer(options ...func(interface{})) (Consumer, error)
	NewProducer(options ...func(interface{})) (Producer, error)
}
