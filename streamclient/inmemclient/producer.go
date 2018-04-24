package inmemclient

import (
	"os"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutil"
	"go.uber.org/zap"
)

// Producer implements the stream.Producer interface for the inmem client.
type Producer struct {
	// c represents the configuration passed into the producer on
	// initialization.
	c streamconfig.Producer

	logger   *zap.Logger
	wg       sync.WaitGroup
	errors   chan error
	messages chan<- stream.Message
	signals  chan os.Signal
	once     *sync.Once
}

var _ stream.Producer = (*Producer)(nil)

// NewProducer returns a new inmem producer.
func NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	ch := make(chan stream.Message)

	producer, err := newProducer(ch, options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We remove this one only after all writes (below)
	// are completed and the write channel is closed.
	producer.wg.Add(1)

	// We start a goroutine to listen for errors on the errors channel, and log a
	// fatal error (terminating the application in the process) when an error is
	// received.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag. If the auto-error functionality is disabled, the user
	// needs to manually listen to the `Errors()` channel and act accordingly.
	if producer.c.HandleErrors {
		go streamutil.HandleErrors(producer.errors, producer.logger.Fatal)
	}

	// We listen to the produce channel in a goroutine. Every message delivered to
	// this producer gets stored in the inmem store. If the producer is closed,
	// the close is blocked until the channel is closed.
	go producer.produce(ch)

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the producer if
	// any such interrupt signal is intercepted. If closing the producer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if producer.c.HandleInterrupt {
		producer.signals = make(chan os.Signal, 1)
		go streamutil.HandleInterrupts(producer.signals, producer.Close, producer.logger)
	}

	return producer, nil
}

// Messages returns the write channel for messages to be produced.
func (p *Producer) Messages() chan<- stream.Message {
	return p.messages
}

// Errors returns the read channel for the errors that are returned by the
// stream.
func (p *Producer) Errors() <-chan error {
	return streamutil.ErrorsChan(p.errors, p.c.HandleErrors)
}

// Close closes the producer connection. This function blocks until all messages
// still in the channel have been processed, and the channel is properly closed.
func (p *Producer) Close() error {
	p.once.Do(func() {
		close(p.messages)

		// Wait until the WaitGroup counter is zero. This makes sure we block the
		// close call until all messages have been delivered, to prevent data-loss.
		p.wg.Wait()

		// At this point, no more errors are expected, so we can close the errors
		// channel.
		close(p.errors)

		// Let's flush all logs still in the buffer, since this producer is no
		// longer useful after this point. We ignore any errors returned by sync, as
		// it is known to return unexpected errors. See: https://git.io/vpJFk
		_ = p.logger.Sync() // nolint: gas

		// Finally, close the signals channel, as it's no longer needed
		close(p.signals)
	})

	return nil
}

// Config returns a read-only representation of the producer configuration as an
// interface. To access the underlying configuration struct, cast the interface
// to `streamconfig.Producer`.
func (p *Producer) Config() interface{} {
	return p.c
}

func (p *Producer) produce(ch <-chan stream.Message) {
	defer p.wg.Done()

	for msg := range ch {
		p.c.Inmem.Store.Add(msg)
	}
}

func newProducer(ch chan stream.Message, options []func(*streamconfig.Producer)) (*Producer, error) {
	config, err := streamconfig.NewProducer(options...)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		c:        config,
		logger:   &config.Logger,
		errors:   make(chan error),
		messages: ch,
		once:     &sync.Once{},
	}

	return producer, nil
}
