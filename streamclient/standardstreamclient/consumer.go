package standardstreamclient

import (
	"bufio"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamcore"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// maxCapacity represents the maximum number of tokens supported per-line. This
// is set to a reasonable high value, to support most use-cases, without
// allocating too much wasted memory. For now, this is hard-coded. If we ever
// have a need, we can move this to the standardstreamconfig package.
const maxCapacity = 512 * 1024

// Consumer implements the stream.Consumer interface for the standard stream
// client.
type Consumer struct {
	// c represents the configuration passed into the consumer on
	// initialization.
	c streamconfig.Consumer

	logger   *zap.Logger
	wg       sync.WaitGroup
	errors   chan error
	messages chan streammsg.Message
}

var _ stream.Consumer = (*Consumer)(nil)

// NewConsumer returns a new standard stream consumer.
func NewConsumer(options ...func(*streamconfig.Consumer)) (stream.Consumer, error) {
	consumer, err := newConsumer(options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all reads (below)
	// are completed and the read channel is closed.
	consumer.wg.Add(1)

	// We start a goroutine to listen for errors on the errors channel, and log a
	// fatal error (terminating the application in the process) when an error is
	// received.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag. If the auto-error functionality is disabled, the user
	// needs to manually listen to the `Errors()` channel and act accordingly.
	if consumer.c.HandleErrors {
		go streamcore.HandleErrors(consumer.errors, consumer.logger.Fatal)
	}

	// We start a goroutine to consume any messages sent to us from the configured
	// reader. We deliver these messages on a blocking channel, so as long as no
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

// Errors returns the read channel for the errors that are returned by the
// stream.
func (c *Consumer) Errors() <-chan error {
	return streamcore.ErrorsChan(c.errors, c.c.HandleErrors)
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
	err := c.c.Standardstream.Reader.Close()
	if err != nil {
		return err
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
	_ = c.logger.Sync() // nolint: gas

	return nil
}

// Config returns a read-only representation of the consumer configuration.
func (c *Consumer) Config() streamconfig.Consumer {
	return c.c
}

func (c *Consumer) consume() {
	// scanner.Scan() stops once it reached the last line of the provided
	// reader. When it does, we close the read channel, making sure any blocking
	// consumers are unblocked. We also reduce the WaitGroup count by one
	// (making the total count zero), making sure we unblock any subsequent call
	// to consumer.Close().
	defer func() {
		close(c.messages)
		c.wg.Done()
	}()

	scanner := bufio.NewScanner(c.c.Standardstream.Reader)
	buf := make([]byte, 0, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		// scanner.Bytes() does not allocate any new memory for the returned
		// bytes. This means that during the next scan, the memory will be re-used
		// for the value of the next line.
		//
		// Since we pass this value to the messages channel, we need to allocate
		// a new permanent copy of the value, to prevent a scenario where the
		// reader of the channel reads the value too late, resulting in unexpected
		// data being returned (race condition).
		b := make([]byte, len(scanner.Bytes()))
		copy(b, scanner.Bytes())

		c.messages <- streammsg.Message{Value: b}
	}

	if err := scanner.Err(); err != nil {
		c.errors <- errors.Wrap(err, "unable to read message from stream")
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
		errors:   make(chan error),
		messages: make(chan streammsg.Message),
	}

	return consumer, nil
}
