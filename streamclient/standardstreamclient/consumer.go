package standardstreamclient

import (
	"bufio"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils"
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
	// config represents the relevant portion of the configuration passed into the
	// consumer its initialization function.
	config standardstreamconfig.Consumer

	// rawConfig represents the as-is configuration passed into the consumer its
	// initialization function by the user. This includes the configuration of
	// other consumer implementations, irrelevant to the current implementation.
	rawConfig streamconfig.Consumer

	logger   *zap.Logger
	wg       sync.WaitGroup
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

	go func() {
		// scanner.Scan() stops once it reached the last line of the provided
		// reader. When it does, we close the read channel, making sure any blocking
		// consumers are unblocked. We also reduce the WaitGroup count by one
		// (making the total count zero), making sure we unblock any subsequent call
		// to consumer.Close().
		defer func() {
			close(consumer.messages)
			consumer.wg.Done()
		}()

		scanner := bufio.NewScanner(consumer.config.Reader)
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

			consumer.messages <- streammsg.Message{Value: b}
		}
	}()

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the consumer if
	// any such interrupt signal is intercepted. If closing the consumer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if consumer.rawConfig.HandleInterrupt {
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
	err := c.config.Reader.Close()
	if err != nil {
		return err
	}

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
		config:    config.Standardstream,
		rawConfig: config,
		logger:    &config.Logger,
		messages:  make(chan streammsg.Message),
	}

	return consumer, nil
}
