package stream

// Client interface to be implemented by different streamclients.
type Client interface {
	NewConsumer() Consumer
	NewProducer() Producer
	NewConsumerAndProducer() (Consumer, Producer)
}
