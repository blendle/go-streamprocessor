package inmemclient

import (
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"go.uber.org/zap"
)

// Consumer implements the stream.Consumer interface for the inmem client.
type Consumer struct {
	// config represents the relevant portion of the configuration passed into the
	// consumer its initialization function.
	config inmemconfig.Consumer

	// rawConfig represents the as-is configuration passed into the consumer its
	// initialization function by the user. This includes the configuration of
	// other consumer implementations, irrelevant to the current implementation.
	rawConfig streamconfig.Consumer

	logger   *zap.Logger
	wg       sync.WaitGroup
	messages chan streammsg.Message
}

var _ stream.Consumer = (*Consumer)(nil)

// NewConsumer returns a new inmem consumer.
func NewConsumer(options ...func(*streamconfig.Consumer)) (stream.Consumer, error) {
	consumer, err := newConsumer(options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all reads (below)
	// are completed and the read channel is closed.
	consumer.wg.Add(1)

	go func() {
		defer func() {
			close(consumer.messages)
			consumer.wg.Done()
		}()

		for _, msg := range consumer.config.Store.Messages() {
			consumer.messages <- msg
		}
	}()

	return consumer, nil
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan streammsg.Message {
	return c.messages
}

// Ack is a no-op implementation to satisfy the stream.Consumer interface.
func (c *Consumer) Ack(_ streammsg.Message) error {
	return nil
}

// Nack is a no-op implementation to satisfy the stream.Consumer interface.
func (c *Consumer) Nack(_ streammsg.Message) error {
	return nil
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	// Wait until the WaitGroup counter is zero. This makes sure we block the
	// close call until the reader has been closed, to prevent reading errors.
	c.wg.Wait()

	return nil
}

// Config returns a read-only representation of the consumer configuration.
func (c *Consumer) Config() streamconfig.Consumer {
	return c.rawConfig
}

func newConsumer(options []func(*streamconfig.Consumer)) (*Consumer, error) {
	config, err := streamconfig.NewConsumer(options...)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		config:    config.Inmem,
		rawConfig: config,
		logger:    &config.Logger,
		messages:  make(chan streammsg.Message),
	}

	return consumer, nil
}
