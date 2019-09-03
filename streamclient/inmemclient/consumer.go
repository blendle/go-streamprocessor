package inmemclient

import (
	"os"
	"sync"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
	"github.com/blendle/go-streamprocessor/v3/streamutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// consumer implements the stream.Consumer interface for the inmem client.
type consumer struct {
	// c represents the configuration passed into the consumer on
	// initialization.
	c streamconfig.Consumer

	logger   *zap.Logger
	messages chan stream.Message
	signals  chan os.Signal
	errors   chan error
	quit     chan bool
	wg       sync.WaitGroup
	once     *sync.Once
}

var _ stream.Consumer = (*consumer)(nil)

// NewConsumer returns a new inmem consumer.
func NewConsumer(options ...streamconfig.Option) (stream.Consumer, error) {
	c, err := newConsumer(options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all reads (below)
	// are completed and the read channel is closed.
	c.wg.Add(1)

	// We start a goroutine to listen for errors on the errors channel, and log a
	// fatal error (terminating the application in the process) when an error is
	// received.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag. If the auto-error functionality is disabled, the user
	// needs to manually listen to the `Errors()` channel and act accordingly.
	if c.c.HandleErrors {
		go streamutil.HandleErrors(c.errors, c.logger.Fatal)
	}

	// We start a goroutine to consume any messages currently stored in the inmem
	// storage. We deliver these messages on a blocking channel, so as long as no
	// one is listening on the other end of the channel, there's no significant
	// overhead to starting the goroutine this early.
	go c.consume()

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the consumer if
	// any such interrupt signal is intercepted. If closing the consumer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if c.c.HandleInterrupt {
		go streamutil.HandleInterrupts(c.signals, c.Close, c.logger)
	}

	return c, nil
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *consumer) Messages() <-chan stream.Message {
	return c.messages
}

// Errors returns the read channel for the errors that are returned by the
// stream.
func (c *consumer) Errors() <-chan error {
	return streamutil.ErrorsChan(c.errors, c.c.HandleErrors)
}

// Ack is a no-op implementation to satisfy the stream.Consumer interface.
func (c *consumer) Ack(_ stream.Message) error {
	return nil
}

// Nack is a no-op implementation to satisfy the stream.Consumer interface.
func (c *consumer) Nack(_ stream.Message) error {
	return nil
}

// Close closes the consumer connection.
func (c *consumer) Close() error {
	c.once.Do(func() {
		if !c.c.Inmem.ConsumeOnce {
			// Trigger the quit channel, which terminates our internal goroutine to
			// process messages, and closes the messages channel.
			c.quit <- true
		}

		// Wait until the WaitGroup counter is zero. This makes sure we block the
		// close call until the reader has been closed, to prevent reading errors.
		c.wg.Wait()

		// At this point, no more errors are expected, so we can close the errors
		// channel.
		close(c.errors)

		// Let's flush all logs still in the buffer, since this consumer is no
		// longer useful after this point. We ignore any errors returned by sync, as
		// it is known to return unexpected errors. See: https://git.io/vpJFk
		_ = c.logger.Sync() // nolint

		// Finally, close the signals channel, as it's no longer needed
		close(c.signals)
	})

	return nil
}

// Backlog returns the number of messages still in the store. Note that this
// value always returns `0`, unless you explicitly set the
// `streamconfig.InmemStream()` option for the consumer, allowing it to
// automatically delete messages in the store once they are consumed.
func (c *consumer) Backlog() (int, error) {
	// If `ConsumeOnce` is set to true, there's no notion of a backlog for the
	// consumer, as we don't keep track of the current offset.
	if c.c.Inmem.ConsumeOnce {
		return 0, nil
	}

	// If `ConsumeOnce` is set to false, consumed messages are deleted from the
	// in-memory store, so we can use the current size of the store as the backlog
	// of messages still to be consumed.
	return len(c.c.Inmem.Store.Messages()), nil
}

// Config returns a read-only representation of the consumer configuration as an
// interface. To access the underlying configuration struct, cast the interface
// to `streamconfig.Consumer`.
func (c *consumer) Config() interface{} {
	return c.c
}

func (c *consumer) consume() {
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

	// If `ConsumeOnce` is set to false, we'll start an infinite loop that listens
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
				select {
				case <-c.quit:
					c.logger.Info("Received quit signal while waiting to deliver " +
						"message to messages channel. Exiting consumer.")

					return
				case c.messages <- msg:
					if err := c.c.Inmem.Store.Del(msg); err != nil {
						c.errors <- errors.Wrap(err, "unable to delete message")
					}
				}
			}
		}
	}
}

func newConsumer(options []streamconfig.Option) (*consumer, error) {
	config, err := streamconfig.NewConsumer(options...)
	if err != nil {
		return nil, err
	}

	c := &consumer{
		c:        config,
		logger:   config.Logger,
		messages: make(chan stream.Message),
		errors:   make(chan error),
		quit:     make(chan bool),
		once:     &sync.Once{},
		signals:  make(chan os.Signal, 3),
	}

	return c, nil
}
