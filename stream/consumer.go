package stream

import (
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

// Consumer interface to be implemented by different stream clients.
type Consumer interface {
	Messages() <-chan streammsg.Message
	Close() error
	Config() streamconfig.Consumer
}
