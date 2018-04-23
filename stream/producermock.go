package stream

// ProducerMock is a mock implementation of the Producer interface
type ProducerMock struct {
	Configuration interface{}
	MessagesChan  chan Message
	ErrorsChan    chan error
}

var _ Producer = (*ProducerMock)(nil)

// Messages implements the Producer interface for ProducerMock.
func (p *ProducerMock) Messages() chan<- Message {
	return p.MessagesChan
}

// Errors implements the Producer interface for ProducerMock.
func (p *ProducerMock) Errors() <-chan error {
	return p.ErrorsChan
}

// Close implements the Producer interface for ProducerMock.
func (p *ProducerMock) Close() error {
	close(p.MessagesChan)
	close(p.ErrorsChan)
	return nil
}

// Config implements the Producer interface for ProducerMock.
func (p ProducerMock) Config() interface{} {
	return p.Configuration
}
