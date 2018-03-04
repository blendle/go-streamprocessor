package inmemclient

import (
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"go.uber.org/zap"
)

// Producer implements the stream.Producer interface for the inmem client.
type Producer struct {
	// config represents the relevant portion of the configuration passed into the
	// producer its initialization function.
	config inmemconfig.Producer

	// rawConfig represents the as-is configuration passed into the producer its
	// initialization function by the user. This includes the configuration of
	// other producer implementations, irrelevant to the current implementation.
	rawConfig streamconfig.Producer

	logger   *zap.Logger
	wg       sync.WaitGroup
	messages chan<- streammsg.Message
}

var _ stream.Producer = (*Producer)(nil)

// NewProducer returns a new inmem producer.
func NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	ch := make(chan streammsg.Message)

	producer, err := newProducer(ch, options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all writes (below)
	// are completed and the write channel is closed.
	producer.wg.Add(1)

	go func() {
		defer producer.wg.Done()

		for msg := range ch {
			producer.config.Store.Add(msg)
		}
	}()

	return producer, nil
}

// Messages returns the write channel for messages to be produced.
func (p *Producer) Messages() chan<- streammsg.Message {
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

func newProducer(ch chan streammsg.Message, options []func(*streamconfig.Producer)) (*Producer, error) {
	config, err := streamconfig.NewProducer(options...)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		config:    config.Inmem,
		rawConfig: config,
		logger:    &config.Logger,
		messages:  ch,
	}

	return producer, nil
}
