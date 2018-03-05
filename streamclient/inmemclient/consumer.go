package inmemclient

import (
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils"
	"go.uber.org/zap"
)

// Consumer implements the stream.Consumer interface for the inmem client.
type Consumer struct {
	// c represents the configuration passed into the consumer on
	// initialization.
	c streamconfig.Consumer

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

	// We start a goroutine to consume any messages currently stored in the inmem
	// storage. We deliver these messages on a blocking channel, so as long as no
	// one is listening on the other end of the channel, there's no significant
	// overhead to starting the goroutine this early.
	go consumer.consume()

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the consumer if
	// any such interrupt signal is intercepted. If closing the consumer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if consumer.c.HandleInterrupt {
		go streamutils.HandleInterrupts(consumer.Close, consumer.logger)
	}

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
	return c.c
}

func (c *Consumer) consume() {
	defer func() {
		close(c.messages)
		c.wg.Done()
	}()

	for _, msg := range c.c.Inmem.Store.Messages() {
		c.messages <- msg
	}
}

func newConsumer(options []func(*streamconfig.Consumer)) (*Consumer, error) {
	config, err := streamconfig.NewConsumer(options...)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		c:        config,
		logger:   &config.Logger,
		messages: make(chan streammsg.Message),
	}

	return consumer, nil
}
