package kafkaclient

import (
	"fmt"
	"os"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// producer implements the stream.Producer interface for the Kafka client.
type producer struct {
	// c represents the configuration passed into the producer on
	// initialization.
	c streamconfig.Producer

	logger   *zap.Logger
	kafka    *kafka.Producer
	wg       sync.WaitGroup
	errors   chan error
	messages chan<- stream.Message
	signals  chan os.Signal
	quit     chan bool
	once     *sync.Once
}

var _ stream.Producer = (*producer)(nil)

// NewProducer returns a new Kafka producer.
func NewProducer(options ...streamconfig.Option) (stream.Producer, error) {
	ch := make(chan stream.Message)

	p, err := newProducer(ch, options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We reduce this count once Close() is called, and
	// the messages channel is closed.
	p.wg.Add(1)

	// We start a goroutine to listen for errors on the errors channel, and log a
	// fatal error (terminating the application in the process) when an error is
	// received.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag. If the auto-error functionality is disabled, the user
	// needs to manually listen to the `Errors()` channel and act accordingly.
	if p.c.HandleErrors {
		go streamutil.HandleErrors(p.errors, p.logger.Fatal)
	}

	// We listen to the produce channel in a goroutine. Every message delivered to
	// this producer gets prepared for a Kafka deliver, and then added to a queue
	// of messages ready to be sent to Kafka. This queue is handled asynchronously
	// for us. If the producer is closed, the close is blocked until the queue is
	// emptied. If the queue can't be emptied, the close call returns an error.
	go p.produce(ch)

	// Each delivered message to Kafka also triggers an event being returned to
	// the producer, with the status of the delivered message. We listen for these
	// reports, and if the report contains an error, we terminate the application,
	// as this puts us into an unknown state from which we cannot recover. We
	// could improve this logic in the future, to provide the user with an events
	// channel of our own, and leave it up to the user what to do in case of such
	// an event.
	go p.checkReports()

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the producer if
	// any such interrupt signal is intercepted. If closing the producer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if p.c.HandleInterrupt {
		go streamutil.HandleInterrupts(p.signals, p.Close, p.logger)
	}

	return p, nil
}

// Messages returns the write channel for messages to be produced.
func (p *producer) Messages() chan<- stream.Message {
	return p.messages
}

// Errors returns the read channel for the errors that are returned by the
// stream.
func (p *producer) Errors() <-chan error {
	return streamutil.ErrorsChan(p.errors, p.c.HandleErrors)
}

// Close closes the producer connection. This function blocks until all messages
// still in the channel have been processed, and the channel is properly closed.
// Close is safe to call more than once, but it will only effectively close the
// producer on the first call.
func (p *producer) Close() (err error) {
	p.once.Do(func() {
		// Trigger the quit channel, which terminates our internal goroutine to
		// process messages, and closes the messages channel. We do this first, to
		// prevent sending any left-over messages to a closed rdkafka producer
		// channel.
		p.quit <- true

		// Wait until the WaitGroup counter is zero. This makes sure we block the
		// close call until the writer has been closed, to prevent reading errors.
		p.wg.Wait()

		// After we are guaranteed to no longer deliver any messages to the rdkafka
		// producer channel, we make sure we flush any messages still waiting to be
		// delivered to Kafka. We allow for a reasonable amount of time to pass
		// before we abort the flush operation. If any messages are still not
		// delivered after the timeout expires, we return an error, indicating that
		// something went wrong.
		i := p.kafka.Flush(5000)
		if i > 0 {
			err = fmt.Errorf("failed to flush all messages, %d left", i)
			return
		}

		// This synchronous call closes the Kafka producer. There are no errors to
		// handle from this close call, unlike the consumer's Close() method.
		p.kafka.Close()

		// At this point, no more errors are expected, so we can close the errors
		// channel.
		close(p.errors)

		// Let's flush all logs still in the buffer, since this producer is no
		// longer useful after this point. We ignore any errors returned by sync, as
		// it is known to return unexpected errors. See: https://git.io/vpJFk
		_ = p.logger.Sync() // nolint: gosec

		// Finally, close the signals channel, as it's no longer needed
		close(p.signals)
	})

	return err
}

// Config returns a read-only representation of the producer configuration as an
// interface. To access the underlying configuration struct, cast the interface
// to `streamconfig.Producer`.
func (p *producer) Config() interface{} {
	return p.c
}

func (p *producer) produce(ch <-chan stream.Message) {
	defer func() {
		close(p.messages)
		p.wg.Done()
	}()

	for {
		select {
		case <-p.quit:
			p.logger.Info("Received quit signal. Exiting producer.")
			return

		case m := <-ch:
			msg := p.newMessage(m)

			// We're using the synchronous `Produce` function instead of the channel-
			// based `ProduceChannel` function, since we want to make sure the message
			// was delivered to the queue. Please note that this does _not_ mean that
			// we wait for the message to actually be delivered to Kafka. rdkafka uses
			// an internal queue and thread to periodically deliver messages to the
			// Kafka broker, and reports back delivery messages on a separate channel.
			// These delivery reports are handled by us in `p.checkReports()`.
			err := p.kafka.Produce(msg, nil)
			if err != nil {
				p.errors <- errors.Wrap(err, "unable to produce message")
			}
		}
	}
}

func newProducer(ch chan stream.Message, options []streamconfig.Option) (*producer, error) {
	// Construct a full configuration object, based on the provided configuration,
	// the default configurations, and the static configurations.
	config, err := streamconfig.NewProducer(options...)
	if err != nil {
		return nil, err
	}

	// Convert the configuration struct into a format that can be sent to the
	// rdkafka library.
	kconfig, err := config.Kafka.ConfigMap()
	if err != nil {
		return nil, err
	}

	config.Logger.Info(
		"Finished parsing Kafka client configurations.",
		zap.Any("config", kconfig),
	)

	// Instantiate a new rdkafka-based Kafka producer.
	kafkaproducer, err := kafka.NewProducer(kconfig)
	if err != nil {
		return nil, err
	}

	p := &producer{
		c:        config,
		logger:   config.Logger,
		kafka:    kafkaproducer,
		errors:   make(chan error),
		messages: ch,
		quit:     make(chan bool),
		once:     &sync.Once{},
		signals:  make(chan os.Signal, 3),
	}

	return p, nil
}

func (p *producer) newMessage(m stream.Message) *kafka.Message {
	headers := make([]kafka.Header, len(m.Tags))
	for k, v := range m.Tags {
		headers = append(headers, kafka.Header{Key: k, Value: v})
	}

	msg := &kafka.Message{
		Value:          m.Value,
		Key:            m.Key,
		Timestamp:      m.Timestamp,
		TopicPartition: p.newToppar(m),
		Headers:        headers,
	}

	return msg
}

// newToppar creates a new `kafka.TopicPartition` object to be used when sending
// a message to the Kafka broker. This topic/partition combination consists of
// either the topic as defined on the to-be-delivered message, _or_ the default
// topic configured for this producer, if an empty topic is set for the message.
// The partition is set to `kafka.PartitionAny`, to allow the Kafka broker to
// determine in which partition the message should end up, based on the key set
// for the message.
func (p *producer) newToppar(m stream.Message) kafka.TopicPartition {
	topic := &p.c.Kafka.Topic
	if m.ProducerTopic != "" {
		topic = &m.ProducerTopic
	}

	return kafka.TopicPartition{
		Topic:     topic,
		Partition: kafka.PartitionAny, // nolint: gotype
	}
}

// checkReports listens to Kafka events send to the producer, and delegates them
// to the appropriate handlers.
func (p *producer) checkReports() {
	for event := range p.kafka.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			p.handleMessage(e)
		case kafka.Error:
			p.handleError(e)
		case *kafka.Stats:
			p.handleStats(e)
		}
	}
}
