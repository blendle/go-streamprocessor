package stream

// ConsumerMock is a mock implementation of the Consumer interface
type ConsumerMock struct {
	Configuration interface{}
	MessagesChan  chan Message
	ErrorsChan    chan error
}

var _ Consumer = (*ConsumerMock)(nil)

// Messages implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Messages() <-chan Message {
	return c.MessagesChan
}

// Errors implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Errors() <-chan error {
	return c.ErrorsChan
}

// Ack implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Ack(_ Message) error {
	return nil
}

// Nack implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Nack(_ Message) error {
	return nil
}

// Close implements the Consumer interface for ConsumerMock.
func (c *ConsumerMock) Close() error {
	close(c.MessagesChan)
	close(c.ErrorsChan)
	return nil
}

// Config implements the Consumer interface for ConsumerMock.
func (c ConsumerMock) Config() interface{} {
	return c.Configuration
}
