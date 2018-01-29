package stream

import "github.com/blendle/go-streamprocessor/streamconfig"

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	Messages() <-chan *Message
	Close() error
	Config() streamconfig.Consumer
}
