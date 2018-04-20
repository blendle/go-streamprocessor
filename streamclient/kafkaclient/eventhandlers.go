package kafkaclient

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
)

const (
	// eventAssignedPartitions represents the assigned partition set for this
	// client following a rebalance. Requires `go.application.rebalance.enable`.
	eventAssignedPartitions = "AssignedPartitions"

	// eventRevokedPartitions represents the counter part to `AssignedPartitions`
	// following a rebalance. `AssignedPartitions` and `RevokedPartitions` are
	// symmetrical. Requires `go.application.rebalance.enable`.
	eventRevokedPartitions = "RevokedPartitions"

	// eventOffsetsCommitted represents the offset commit results (when
	// `enable.auto.commit` is enabled).
	eventOffsetsCommitted = "OffsetsCommitted"

	// eventError represents a client (error codes are prefixed with _) or broker
	// error. These errors are normally just informational since the client will
	// try its best to automatically recover (eventually).
	eventError = "Error"

	// eventMessage has two meanings, depending on if it's sent to the consumer or
	// the producer:
	//
	// consumer: a fetched message.
	// producer: delivery report for produced message.
	eventMessage = "Message"
)

// handleAssignedPartitions assigns the consumer to a provided partition.
func (c *Consumer) handleAssignedPartitions(e kafka.AssignedPartitions) {
	log := c.logger.With(
		zap.String("eventType", eventAssignedPartitions),
		zap.String("eventDetails", e.String()),
	)

	log.Info("Received event from Kafka.")

	err := c.kafka.Assign(e.Partitions)
	if err != nil {
		log.Fatal(
			"Error while assigning partitions.",
			zap.Error(err),
		)
	}
}

// handleRevokedPartitions unassigns the consumer from a provided partition.
func (c *Consumer) handleRevokedPartitions(e kafka.RevokedPartitions) {
	log := c.logger.With(
		zap.String("eventType", eventRevokedPartitions),
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
		log.Fatal(
			"Error while unassigning partitions. Failed to commit offsets before unassignment.",
			zap.Error(err),
		)
	}

	log.Debug(
		"Successfully committed offsets before unassignment.",
		zap.Any("partitionDetails", p),
	)

	err = c.kafka.Unassign()
	if err != nil {
		log.Fatal(
			"Error while unassigning partitions. Failed to unassign partitions.",
			zap.Error(err),
		)
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
		zap.String("eventType", eventOffsetsCommitted),
		zap.String("eventDetails", e.String()),
		zap.Any("offsets", e.Offsets),
		zap.Error(e.Error),
	)
}

// handleError handles all error events for the consumer.
func (c *Consumer) handleError(e kafka.Error) {
	handleError(c.logger, e)
}

// handleError handles all error events for the producer.
func (p *Producer) handleError(e kafka.Error) {
	handleError(p.logger, e)
}

// handleError handles all error events for both the producer and consumer. If
// an error event is received, we terminate the application, as this brings us
// into an unknown and potentially unrecoverable state.
//
// TODO: the rdkafka documentation states the following:
//
//       > These errors are normally just informational since the client will
//       > try its best to automatically recover (eventually).
//
//       We'll monitor this and see if we need to change this in the future. We
//       are also considering adding an `Events()` channel of our own, so
//       perhaps we don't have to handle this situation ourselves, and the
//       application can dictate what they want to do in the case of an error.
func handleError(logger *zap.Logger, e kafka.Error) {
	logger.Fatal(
		"Received event from Kafka.",
		zap.String("eventType", eventError),
		zap.String("eventDetails", e.String()),
		zap.Error(e),
	)
}

// handleMessage handles all Kafka messages by converting the message to a
// `streammsg.Message` format, and delivers it to the receiver using the
// messages channel. The return value indicates whether or not the quit signal
// was received while waiting to deliver the message. This value is used by the
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

	p.logger.Error(
		"Received failed message delivery event from Kafka.",
		zap.String("eventType", eventMessage),
		zap.String("eventDetails", e.String()),
		zap.Error(e.TopicPartition.Error),
	)
}
