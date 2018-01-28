package stream

import "github.com/blendle/go-streamprocessor/streamconfig"

// Client interface to be implemented by different stream clients.
type Client interface {
	NewConsumer(options ...func(*streamconfig.Consumer)) (Consumer, error)
	NewProducer(options ...func(*streamconfig.Producer)) (Producer, error)
}
