package stream

import (
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

// Producer interface to be implemented by different stream clients.
type Producer interface {
	NewMessage(value []byte) streammsg.Message
	Messages() chan<- streammsg.Message
	Close() error
	Config() streamconfig.Producer
}
