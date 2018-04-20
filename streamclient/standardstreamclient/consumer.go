package standardstreamclient

import (
	"bufio"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
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

	wg       sync.WaitGroup
	messages chan *stream.Message
}

// NewConsumer returns a new standard stream consumer.
func (c *Client) NewConsumer(options ...func(*streamconfig.Consumer)) (stream.Consumer, error) {
	consumer, err := newConsumer(c, options)
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
			// data being returned.
			b := make([]byte, len(scanner.Bytes()))
			copy(b, scanner.Bytes())

			consumer.messages <- &stream.Message{Value: b}
		}
	}()

	return consumer, nil
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
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

func newConsumer(c *Client, options []func(*streamconfig.Consumer)) (*Consumer, error) {
	config, err := streamconfig.NewConsumer(c.rawConfig, options...)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		config:    config.Standardstream,
		rawConfig: config,
		messages:  make(chan *stream.Message),
	}

	return consumer, nil
}
