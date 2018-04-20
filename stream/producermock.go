package stream

import (
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
)

// ProducerMock is a mock implementation of the Producer interface
type ProducerMock struct {
	Configuration streamconfig.Producer
	MessagesChan  chan streammsg.Message
}


// Messages implements the Producer interface for ProducerMock.
func (p *ProducerMock) Messages() chan<- streammsg.Message {
	return p.MessagesChan
}

// Close implements the Producer interface for ProducerMock.
func (p *ProducerMock) Close() error {
	close(p.MessagesChan)
	return nil
}

// Config implements the Producer interface for ProducerMock.
func (p ProducerMock) Config() streamconfig.Producer {
	return p.Configuration
}
