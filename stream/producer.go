package stream

import "github.com/blendle/go-streamprocessor/streamconfig"

// Producer interface to be implemented by different stream clients.
type Producer interface {
	Messages() chan<- *Message
	Close() error
	Config() streamconfig.Producer
}
