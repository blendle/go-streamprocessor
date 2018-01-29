package standardstreamclient

import (
	"bytes"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
)

// Producer implements the stream.Producer interface for the standard stream
// client.
type Producer struct {
	// config represents the relevant portion of the configuration passed into the
	// producer its initialization function.
	config standardstreamconfig.Producer

	// rawConfig represents the as-is configuration passed into the producer its
	// initialization function by the user. This includes the configuration of
	// other producer implementations, irrelevant to the current implementation.
	rawConfig streamconfig.Producer

	wg       sync.WaitGroup
	messages chan<- *stream.Message
}

// NewProducer returns a new standard stream producer.
func (c *Client) NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	ch := make(chan *stream.Message)

	producer, err := newProducer(c, ch, options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all writes (below)
	// are completed and the write channel is closed.
	producer.wg.Add(1)

	go func() {
		defer producer.wg.Done()

		for msg := range ch {
			message := msg.Value

			// If the original message does not contain a newline at the end, we add
			// it, as this is used as the message delimiter.
			if !bytes.HasSuffix(message, []byte("\n")) {
				message = append(message, "\n"...)
			}

			_, err := producer.config.Writer.Write(message)
			if err != nil {
				panic(err)
			}
		}
	}()

	return producer, nil
}

// Messages returns the write channel for messages to be produced.
func (p *Producer) Messages() chan<- *stream.Message {
	return p.messages
}

// Close closes the producer connection. This function blocks until all messages
// still in the channel have been processed, and the channel is properly closed.
func (p *Producer) Close() error {
	close(p.messages)

	// Wait until the WaitGroup counter is zero. This makes sure we block the
	// close call until all messages have been delivered, to prevent data-loss.
	p.wg.Wait()

	return nil
}

// Config returns a read-only representation of the producer configuration.
func (p *Producer) Config() streamconfig.Producer {
	return p.rawConfig
}

func newProducer(c *Client, ch chan *stream.Message, options []func(*streamconfig.Producer)) (*Producer, error) {
	config, err := streamconfig.NewProducer(c.rawConfig, options...)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		config:    config.Standardstream,
		rawConfig: config,
		messages:  ch,
	}

	return producer, nil
}
