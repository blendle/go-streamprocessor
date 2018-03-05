package kafkaclient

import (
	"fmt"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

// Producer implements the stream.Producer interface for the Kafka client.
type Producer struct {
	// c represents the configuration passed into the producer on
	// initialization.
	c streamconfig.Producer

	logger   *zap.Logger
	kafka    *kafka.Producer
	wg       sync.WaitGroup
	messages chan<- streammsg.Message
	quit     chan bool
	once     *sync.Once
}

var _ stream.Producer = (*Producer)(nil)

// NewProducer returns a new Kafka producer.
func NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	ch := make(chan streammsg.Message)

	producer, err := newProducer(ch, options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We reduce this count once Close() is called, and
	// the messages channel is closed.
	producer.wg.Add(1)

	// We listen to the produce channel in a goroutine. Every message delivered to
	// this producer gets prepared for a Kafka deliver, and then added to a queue
	// of messages ready to be sent to Kafka. This queue is handled asynchronously
	// for us. If the producer is closed, the close is blocked until the queue is
	// emptied. If the queue can't be emptied, the close call returns an error.
	go producer.produce(ch)

	// Each delivered message to Kafka also triggers an event being returned to
	// the producer, with the status of the delivered message. We listen for these
	// reports, and if the report contains an error, we terminate the application,
	// as this puts us into an unknown state from which we cannot recover. We
	// could improve this logic in the future, to provide the user with an events
	// channel of our own, and leave it up to the user what to do in case of such
	// an event.
	go producer.checkReports()

	// Finally, we monitor for any interrupt signals. Ideally, the user handles
	// these cases gracefully, but just in case, we try to close the producer if
	// any such interrupt signal is intercepted. If closing the producer fails, we
	// exit 1, and log a fatal message explaining what happened.
	//
	// This functionality is enabled by default, but can be disabled through a
	// configuration flag.
	if producer.c.HandleInterrupt {
		go streamutils.HandleInterrupts(producer.Close, producer.logger)
	}

	return producer, nil
}

// Messages returns the write channel for messages to be produced.
func (p *Producer) Messages() chan<- streammsg.Message {
	return p.messages
}

// Close closes the producer connection. This function blocks until all messages
// still in the channel have been processed, and the channel is properly closed.
func (p *Producer) Close() (err error) {
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

		// Let's flush all logs still in the buffer, since this producer is no
		// longer useful after this point.
		_ = p.logger.Sync() // nolint: gas
	})

	return err
}

// Config returns a read-only representation of the producer configuration.
func (p *Producer) Config() streamconfig.Producer {
	return p.c
}

func newProducer(ch chan streammsg.Message, options []func(*streamconfig.Producer)) (*Producer, error) {
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

	// Instantiate a new rdkafka-based Kafka producer.
	kafkaproducer, err := kafka.NewProducer(kconfig)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		c:        config,
		logger:   &config.Logger,
		kafka:    kafkaproducer,
		messages: ch,
		quit:     make(chan bool),
		once:     &sync.Once{},
	}

	return producer, nil
}

func (p *Producer) produce(ch <-chan streammsg.Message) {
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
				p.logger.Fatal(
					"Error while delivering message to Kafka.",
					zap.Error(err),
				)
			}
		}
	}
}

func (p *Producer) newMessage(m streammsg.Message) *kafka.Message {
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
func (p *Producer) newToppar(m streammsg.Message) kafka.TopicPartition {
	topic := &p.c.Kafka.Topic
	if m.Topic != "" {
		topic = &m.Topic
	}

	return kafka.TopicPartition{
		Topic:     topic,
		Partition: kafka.PartitionAny, // nolint: gotype
	}
}

// checkReports listens to Kafka events send to the producer, and delegates them
// to the appropriate handlers.
func (p *Producer) checkReports() {
	for event := range p.kafka.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			p.handleMessage(e)
		case kafka.Error:
			p.handleError(e)
		}
	}
}
