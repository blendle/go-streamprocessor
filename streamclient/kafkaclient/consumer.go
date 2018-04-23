package kafkaclient

import (
	"errors"
	"os"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamcore"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

// Consumer implements the `stream.Consumer` interface for the Kafka client.
type Consumer struct {
	// c represents the configuration passed into the consumer on
	// initialization.
	c streamconfig.Consumer

	logger   *zap.Logger
	kafka    *kafka.Consumer
	wg       sync.WaitGroup
	errors   chan error
	messages chan stream.Message
	signals  chan os.Signal
	quit     chan bool
	once     *sync.Once
}

type opaque struct {
	toppar *kafka.TopicPartition
}

var _ stream.Consumer = (*Consumer)(nil)

// NewConsumer returns a new Kafka consumer.
func NewConsumer(options ...func(*streamconfig.Consumer)) (stream.Consumer, error) {
	consumer, err := newConsumer(options)
	if err != nil {
		return nil, err
	}

	// add one to the WaitGroup. We reduce this count once Close() is called, and
	// the messages channel is closed.
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

	// We start a goroutine to consume any messages being delivered to us from
	// Kafka. We deliver these messages on a blocking channel, so as long as no
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
		consumer.signals = make(chan os.Signal, 1)
		go streamcore.HandleInterrupts(consumer.signals, consumer.Close, consumer.logger)
	}

	return consumer, nil
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan stream.Message {
	return c.messages
}

// Errors returns the read channel for the errors that are returned by the
// stream.
func (c *Consumer) Errors() <-chan error {
	return streamcore.ErrorsChan(c.errors, c.c.HandleErrors)
}

// Ack acknowledges that a message was processed. See `Consumer.storeOffset` for
// more details on how message acknowledgment works for the Kafka consumer. If
// the consumer is unable to acknowledge a message, an error is returned.
func (c *Consumer) Ack(m stream.Message) error {
	o, ok := stream.MessageOpqaue(&m).(opaque)
	if !ok {
		return errors.New("unsuccessful type assertion")
	}

	return c.storeOffset(*o.toppar)
}

// Nack is a no-op implementation to satisfy the `stream.Consumer` interface. We
// don't need an actual implementation, since not acknowledging a message will
// eventually result in the message being redelivered.
func (c *Consumer) Nack(m stream.Message) error {
	return nil
}

// Close closes the consumer connection. Close is safe to call more than once,
// but it will only effectively close the consumer on the first call.
func (c *Consumer) Close() (err error) {
	c.once.Do(func() {
		// This synchronous call closes the Kafka consumer and also sends any
		// still-to-be-committed offsets to the Broker before returning. This is
		// done first, so that no new messages are delivered to us, before we close
		// our own channel.
		err = c.kafka.Close()
		if err != nil {
			return
		}

		// Trigger the quit channel, which terminates our internal goroutine to
		// process messages, and closes the messages channel.
		c.quit <- true

		// Wait until the WaitGroup counter is zero. This makes sure we block the
		// close call until the reader has been closed, to prevent an application
		// from quiting before we are fully done with all the clean-up.
		c.wg.Wait()

		// At this point, no more errors are expected, so we can close the errors
		// channel.
		close(c.errors)

		// we set the quit channel to nil, indicating that this consumer can't be
		// used anymore. There's a potential for race conditions here, but that's
		// not a big issue at this moment, since this is only used to make the
		// internal `storeOffset` into a no-op, which basically means that you are
		// not allowed to hold on to a message, and calling `consumer.Ack(message)`
		// even after calling `consumer.Close()`. This is purely meant to display a
		// more readable explanation of what happened when `Ack` returns an error,
		// instead of throwing a panic because the rdkafka consumer has already been
		// terminated.
		c.quit = nil

		// Let's flush all logs still in the buffer, since this consumer is no
		// longer useful after this point. We ignore any errors returned by sync, as
		// it is known to return unexpected errors. See: https://git.io/vpJFk
		_ = c.logger.Sync() // nolint: gas

		// Finally, close the signals channel, as it's no longer needed
		close(c.signals)
	})

	return err
}

// Config returns a read-only representation of the consumer configuration as an
// interface. To access the underlying configuration struct, cast the interface
// to `streamconfig.Consumer`.
func (c *Consumer) Config() interface{} {
	return c.c
}

func (c *Consumer) consume() {
	defer func() {
		close(c.messages)
		c.wg.Done()
	}()

	for {
		select {
		case <-c.quit:
			c.logger.Info("Received quit signal. Exiting consumer.")

			return
		case event, ok := <-c.kafka.Events():
			if !ok {
				return
			}

			switch e := event.(type) {

			// If we received an `AssignedPartitions` event, we need to make sure we
			// assign the currently running consumer to the right partitions.
			case kafka.AssignedPartitions:
				c.handleAssignedPartitions(e)

			// If we received an `RevokedPartitions` event, we need to revoke this
			// consumer from all partitions. This means this consumer won't pick up
			// any work anymore, until a new `AssignedPartitions` event is handled.
			case kafka.RevokedPartitions:
				c.handleRevokedPartitions(e)

			// OffsetsCommitted lets us know that a partition offset was updated.
			// There is nothing we need to do with this information, but if a server-
			// side error occurred, we can capture this error and log it.
			case kafka.OffsetsCommitted:
				c.handleOffsetCommitted(e)

			// If we receive an error, something happened on Kafka's side. We don't
			// know what happened or if we can recover gracefully, so we instead
			// terminate the running process.
			case kafka.Error:
				c.handleError(e)

			// On receiving a Kafka message, we process the received message and
			// prepare it for delivery to the receiver of the consumer.messages
			// channel.
			//
			// FIXME: this case can be blocking, if no receiver is listening on the
			//        other end of the messages channel. If that's the case, we can't
			//        handle other events anymore until this message is consumed. This
			//        can get the consumer into a deadlock, when calling `Close()`,
			//        where the server is waiting for the consumer to handle the
			//        `RevokedPartitions` event before terminating the connection, and
			//        the consumer not being able to handle that event, due to it
			//        still having a message in the queue, that no receiver is
			//        accepting.
			//
			//        Ideally, the solution would be for the Kafka library to support
			//        different channels for messages and events, but that's not the
			//        case right now. Another solution would be to create a buffered
			//        channel for messages, and make this a non-blocking case
			//        statement, but that brings with it its own set of problems.
			//
			//        see: https://git.io/vAHTg
			case *kafka.Message:
				// handleMessage returns true if a message was received on the `quit`
				// channel while waiting to deliver the Kafka message to the `messages`
				// channel. If this happens, it means the consumer is being closed, so
				// we exit the for loop and this function.
				quitReceived := c.handleMessage(e)
				if quitReceived {
					return
				}
			}
		}
	}
}

func newConsumer(options []func(*streamconfig.Consumer)) (*Consumer, error) {
	// Construct a full configuration object, based on the provided configuration,
	// the default configurations, and the static configurations.
	config, err := streamconfig.NewConsumer(options...)
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

	// Instantiate a new rdkafka-based Kafka consumer.
	kafkaconsumer, err := kafka.NewConsumer(kconfig)
	if err != nil {
		return nil, err
	}

	err = kafkaconsumer.SubscribeTopics(config.Kafka.Topics, nil)
	if err != nil {
		return nil, err
	}

	consumer := &Consumer{
		c:        config,
		logger:   &config.Logger,
		kafka:    kafkaconsumer,
		errors:   make(chan error),
		messages: make(chan stream.Message),
		quit:     make(chan bool, 1),
		once:     &sync.Once{},
	}

	return consumer, nil
}

// newMessageFromKafka takes a *kafka.Message (provided by librdkafka), and
// converts it to this package's `stream.Message` format.
func newMessageFromKafka(m *kafka.Message) *stream.Message {
	oint := int64(m.TopicPartition.Offset)
	offset := &oint

	// If the offset is set to the special-value `-1001`, that means the offset is
	// not set yet (or invalid), so we set the offset to `nil`.
	//
	// see: https://git.io/vAHI2
	if oint == -1001 {
		offset = nil
	}

	msg := &stream.Message{
		Key:       m.Key,
		Value:     m.Value,
		Timestamp: m.Timestamp,
		Topic:     *m.TopicPartition.Topic,
		Offset:    offset,
	}

	// We set the message's opaque field (which is still nil at this point), and
	// populate it with the `TopicPartition` details of the Kafka message. This
	// allows us to acknowledge this message at a later point in time, without
	// having to hold on to the Kafka message itself.
	_ = stream.SetMessageOpaque(msg, opaque{toppar: &m.TopicPartition})

	return msg
}

// storeOffset accepts a `kafka.TopicPartition` and uses the rdkafka-consumer to
// store the offset of that "toppar" in an internal queue. This queue is
// regularly processed by rdkafka, and the results are delivered to the Kafka
// broker. When closing the consumer, one final push is done for any offsets
// still pending in the offset store. This set-up allows us to have a fast
// "acknowledgment" implementation, while still having a very high guarantee of
// offset correctness (the only situation where this can go wrong is in an
// abrupt termination of the process, without any proper notice of termination).
func (c *Consumer) storeOffset(tp kafka.TopicPartition) error {
	// if c.quit equals nil, this means this consumer is no longer in an operable
	// state, and the underlying kafka Consumer has already been closed. In such a
	// situation, we can no longer commit any offsets, and will thus have to
	// return an error, indicating this situation. The receiver of the error can
	// either ignore it, but most likely will want to terminate the application,
	// as there's no longer any guarantee of ordered message delivery.
	if c.quit == nil {
		return errors.New("consumer closed, unable to store offsets")
	}

	// Increase the current offset by one, to indicate this offset was
	// successfully processed.
	tp.Offset++

	_, err := c.kafka.StoreOffsets([]kafka.TopicPartition{tp})

	return err
}

// commit can be used to manually (and synchronously) commit any offsets
// currently stored in the internal offset store. This method is called when
// the Kafka broker sends a partition rebalance request. When this happens, we
// first commit any still-to-be-committed offsets, before we unassign ourselves
// from the partition.
func (c *Consumer) commit() ([]kafka.TopicPartition, error) {
	p, err := c.kafka.Commit()
	if err == nil {
		c.logger.Debug(
			"Committed local partition offsets to broker.",
			zap.Any("partitionDetails", p),
		)

		return p, nil
	}

	// ErrNoOffset Local: No offset stored
	//
	// This error can be ignored, as it simply means there was nothing to commit.
	kerr, ok := err.(kafka.Error)
	if ok && kerr.Code() == kafka.ErrNoOffset {
		err = nil
	}

	return p, err
}
