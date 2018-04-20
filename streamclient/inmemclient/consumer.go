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
	messages chan streammsg.Message
	quit     chan bool
	wg       sync.WaitGroup
	once     *sync.Once
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
	c.once.Do(func() {
		if !c.c.Inmem.ConsumeOnce {
			// Trigger the quit channel, which terminates our internal goroutine to
			// process messages, and closes the messages channel.
			c.quit <- true
		}

		// Wait until the WaitGroup counter is zero. This makes sure we block the
		// close call until the reader has been closed, to prevent reading errors.
		c.wg.Wait()

		// Let's flush all logs still in the buffer, since this consumer is no
		// longer useful after this point. We ignore any errors returned by sync, as
		// it is known to return unexpected errors. See: https://git.io/vpJFk
		_ = c.logger.Sync() // nolint: gas
	})

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

	// If `ConsumeOnce` is set to true, we simply loop over all the existing
	// messages in the inmem store and send them to the consumer channel. This
	// will result in this `consume()` method to return once all messages are
	// delivered to the channel.
	if c.c.Inmem.ConsumeOnce {
		for _, msg := range c.c.Inmem.Store.Messages() {
			c.messages <- msg
		}

		return
	}

	// If `ConsumeOnce` is set to true, we'll start an infinite loop that listens
	// to new messages in the inmem store, and send them to the consumer channel.
	// Once a message is read from the store, it's also deleted from the store, so
	// that that message is not delivered twice.
	//
	// TODO: we might consider implementing `Ack` to actually delete the message
	//       from the store, which would create a more true-to-spirit
	//       implementation of a stream client, instead of having the side-effect
	//       of actually removing the message from the store happening in this
	//       method.
	for {
		select {
		case <-c.quit:
			c.logger.Info("Received quit signal. Exiting consumer.")

			return
		default:
			for _, msg := range c.c.Inmem.Store.Messages() {
				c.messages <- msg
				c.c.Inmem.Store.Delete(msg)
			}
		}
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
		quit:     make(chan bool),
		once:     &sync.Once{},
	}

	return consumer, nil
}
