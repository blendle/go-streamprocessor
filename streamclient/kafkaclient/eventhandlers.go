package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// handleAssignedPartitions assigns the consumer to a provided partition.
func (c *Consumer) handleAssignedPartitions(e kafka.AssignedPartitions) {
	c.logger.Info(
		"Received event from Kafka.",
		zap.String("eventType", "AssignedPartitions"),
		zap.String("eventDetails", e.String()),
	)

	err := c.kafka.Assign(e.Partitions)
	if err != nil {
		// Ignore the `Local: Broker handle destroyed` error.
		//
		// See: https://git.io/vpt2L
		// See: https://git.io/vpt2y
		if err.(kafka.Error).Code() == kafka.ErrDestroy {
			return
		}

		c.errors <- errors.Wrap(err, "failed to assinging partitions")
	}
}

// handleRevokedPartitions unassigns the consumer from a provided partition.
func (c *Consumer) handleRevokedPartitions(e kafka.RevokedPartitions) {
	log := c.logger.With(
		zap.String("eventType", "RevokedPartitions"),
		zap.String("eventDetails", e.String()),
	)

	log.Info("Received event from Kafka.")

	// Before revoking the currently managed partitions, we make sure to call
	// commit one final time synchronously, to drain the offset store of any
	// offsets that still need to be committed to Kafka. If any error occurs
	// during this final offset commitment, we terminate hard, making sure we can
	// reprocess any messages that where lost in the store.
	p, err := c.commit()
	if err != nil {
		c.errors <- errors.Wrap(err, "failed to commit offsets before partition unassignment")
	}

	log.Debug(
		"Successfully committed offsets before unassignment.",
		zap.Any("partitionDetails", p),
	)

	err = c.kafka.Unassign()
	if err != nil {
		c.errors <- errors.Wrap(err, "failed to unassign partitions")
	}
}

// handleOffsetCommitted handles the return-message received after committing
// offsets to the Kafka broker. In almost all cases, this is a no-op, but if the
// returned message actually contains an error, we log that error, but don't
// crash, as there's nothing we can do at this point, since the offset is
// already delivered to Kafka.
func (c *Consumer) handleOffsetCommitted(e kafka.OffsetsCommitted) {
	if e.Error == nil {
		return
	}

	c.logger.Error(
		"OffsetsCommitted event returned error.",
		zap.String("eventDetails", e.String()),
		zap.Any("offsets", e.Offsets),
		zap.Error(e.Error),
	)
}

// handleError handles all error events for the consumer. If an error event is
// received, we terminate the application, as this brings us into an unknown and
// potentially unrecoverable state.
//
// NOTE: the rdkafka documentation states the following:
//
//   > These errors are normally just informational since the client will
//   > try its best to automatically recover (eventually).
//
// We'll monitor this and see if we need to change this in the future. If you
// want to handle this situation manually, use the `Events()` method to receive
// the raised errors.
func (c *Consumer) handleError(e kafka.Error) {
	c.errors <- errors.Wrap(e, "received error from event stream")
}

// handleError handles all error events for the producer. If an error event is
// received, we terminate the application, as this brings us into an unknown and
// potentially unrecoverable state.
//
// NOTE: the rdkafka documentation states the following:
//
//   > These errors are normally just informational since the client will
//   > try its best to automatically recover (eventually).
//
// We'll monitor this and see if we need to change this in the future. If you
// want to handle this situation manually, use the `Events()` method to receive
// the raised errors.
func (p *Producer) handleError(e kafka.Error) {
	p.errors <- errors.Wrap(e, "received error from event stream")
}

// handleMessage handles all Kafka messages by converting the message to a
// `stream.Message` format, and delivers it to the receiver using the messages
// channel. The return value indicates whether or not the quit signal was
// received while waiting to deliver the message. This value is used by the
// consumer to close up shop.
func (c *Consumer) handleMessage(e *kafka.Message) bool {
	msg := newMessageFromKafka(e)

	// Once the message has been prepared, we offer it to the consumer of
	// the messages channel. Since this is a blocking channel, we also
	// listen for the quit signal, and stop delivering new messages
	// accordingly.
	select {
	case c.messages <- *msg:
	case <-c.quit:
		c.logger.Info("Received quit signal while waiting to deliver " +
			"Kafka message to messages channel. Exiting consumer.")

		return true
	}

	return false
}

// As a producer, we also listen for *kafka.Message events, but these events are
// only relevant to validate that a published message was actually delivered as
// expected. We check the error state of the message, and if there's an error,
// we terminate the program, as there is no way to recover from this situation.
func (p *Producer) handleMessage(e *kafka.Message) {
	if e.TopicPartition.Error == nil {
		return
	}

	p.errors <- errors.Wrap(e.TopicPartition.Error, "message delivery failure error")
}
