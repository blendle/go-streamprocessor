package kafkaclient

import (
	"encoding/json"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// handleAssignedPartitions assigns the consumer to a provided partition.
func (c *consumer) handleAssignedPartitions(e kafka.AssignedPartitions) {
	c.logger.Info(
		"Received event from Kafka.",
		zap.String("eventType", "AssignedPartitions"),
		zap.String("eventDetails", e.String()),
	)

	tps := make([]kafka.TopicPartition, len(e.Partitions))
	for i := range e.Partitions {
		tps[i] = e.Partitions[i]
	}

	// If OffsetDefault is set to anything other than nil, this consumer is
	// configured to receive data from a either a positive (starting from the
	// beginning), or negative offset (starting from the end). In this case, we
	// manually override the starting offset of the partition when we receive an
	// assigned partition request, but only if no offset is stored yet at the
	// Kafka brokers.
	if c.c.Kafka.OffsetDefault != nil {
		// Set offsets, to support custom initial offsets. We start by setting the
		// offset as usual, then we convert the offset to a "tail" type if we're
		// actually dealing with a negative integer.
		//
		// see: https://git.io/vpa3B
		offset := kafka.Offset(*c.c.Kafka.OffsetDefault)
		if *c.c.Kafka.OffsetDefault < 0 {
			offset = kafka.OffsetTail(-offset)
		}

		// Fetch already known partition offsets from Kafka, to check if we've
		// already consumed messages with this consumer group.
		ctps, err := c.kafka.Committed(tps, 5000)
		if err != nil {
			c.errors <- errors.Wrap(err, "unable to retrieve committed offsets")
			return
		}

		for i := range tps {
			// If the offset stored at Kafka is _not_ of type "Invalid", it means this
			// consumer group has consumed messages before, and so we ignore the
			// `OffsetDefault` configuration, and instead continue reading from where
			// we left off...
			if ctps[i].Offset != kafka.OffsetInvalid {
				continue
			}

			// ...otherwise we set the partition offset to the default configured for
			// this consumer, to start receiving messages starting from the configured
			// offset.
			tps[i].Offset = offset
		}
	}

	err := c.kafka.Assign(tps)
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
func (c *consumer) handleRevokedPartitions(e kafka.RevokedPartitions) {
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
func (c *consumer) handleOffsetCommitted(e kafka.OffsetsCommitted) {
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
// received, we either terminate the application, as this brings us into an
// unknown and potentially unrecoverable state, or we log, but ignore an error,
// if the error code is part of the configured `IgnoreErrors` list.
//
// The rdkafka documentation states the following:
//
//   > These errors are normally just informational since the client will
//   > try its best to automatically recover (eventually).
//
// See `IgnoreErrors` for the errors we ignore by default. Use
// `streamconfig.KafkaHandleTransientErrors()` to handle _all_ errors, even the
// ones that can be resolved automatically.
//
// If you want to handle (non-ignored) errors manually, set
// `streamconfig.ManualErrorHandling()`, and use the `Events()` method to
// receive the raised errors.
func (c *consumer) handleError(e kafka.Error) {
	handleError(e, c.c.Kafka.IgnoreErrors, c.errors, c.logger)
}

// handleError handles all error events for the producer. If an error event is
// received, we either terminate the application, as this brings us into an
// unknown and potentially unrecoverable state, or we log, but ignore an error,
// if the error code is part of the configured `IgnoreErrors` list.
//
// The rdkafka documentation states the following:
//
//   > These errors are normally just informational since the client will
//   > try its best to automatically recover (eventually).
//
// See `IgnoreErrors` for the errors we ignore by default. Use
// `streamconfig.KafkaHandleTransientErrors()` to handle _all_ errors, even the
// ones that can be resolved automatically.
//
// If you want to handle (non-ignored) errors manually, set
// `streamconfig.ManualErrorHandling()`, and use the `Events()` method to
// receive the raised errors.
func (p *producer) handleError(e kafka.Error) {
	handleError(e, p.c.Kafka.IgnoreErrors, p.errors, p.logger)
}

func handleError(err kerr, ignores []kafka.ErrorCode, ch chan error, logger *zap.Logger) {
	for i := range ignores {
		if ignores[i] != err.Code() {
			continue
		}

		logger.Warn(
			"Received transient error from Kafka. Ignoring.",
			zap.Error(err),
		)

		return
	}

	select {
	case ch <- errors.Wrap(err, "received error from event stream"):
	default:
		logger.Error(
			"Unable to deliver Kafka error. "+
				"Either you are not listening to the errors channel, or this is a bug.",
			zap.Error(err),
		)
	}
}

type kerr interface {
	Error() string
	Code() kafka.ErrorCode
}

// handleStats handles all statistic events for the consumer. These statistics
// are periodically delivered (based off of the `StatisticsInterval`
// configuration value), and are then logged at level "INFO" (if the logger is
// configured to log INFO-level messages).
func (c *consumer) handleStats(e *kafka.Stats) {
	c.logger.Info(
		"Received Kafka statistics.",
		zap.Any("statistics", json.RawMessage(e.String())),
		zap.String("details", "https://github.com/edenhill/librdkafka/wiki/Statistics"),
	)
}

// handleStats handles all statistic events for the producer. These statistics
// are periodically delivered (based off of the `StatisticsInterval`
// configuration value), and are then logged at level "INFO" (if the logger is
// configured to log INFO-level messages).
func (p *producer) handleStats(e *kafka.Stats) {
	p.logger.Info(
		"Received Kafka statistics.",
		zap.Any("statistics", json.RawMessage(e.String())),
		zap.String("details", "https://github.com/edenhill/librdkafka/wiki/Statistics"),
	)
}

// handleMessage handles all Kafka messages by converting the message to a
// `stream.Message` format, and delivers it to the receiver using the messages
// channel. The return value indicates whether or not the quit signal was
// received while waiting to deliver the message. This value is used by the
// consumer to close up shop.
func (c *consumer) handleMessage(e *kafka.Message) bool {
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
func (p *producer) handleMessage(e *kafka.Message) {
	if e.TopicPartition.Error == nil {
		return
	}

	p.errors <- errors.Wrap(e.TopicPartition.Error, "message delivery failure error")
}
