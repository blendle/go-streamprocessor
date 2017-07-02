package kafka

import (
	"github.com/blendle/go-streamprocessor/stream"
	cluster "github.com/bsm/sarama-cluster"
	"go.uber.org/zap"
)

// NewConsumer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewConsumer() stream.Consumer {
	kafkaconsumer, err := cluster.NewConsumer(
		c.ConsumerBrokers,
		c.ConsumerGroup,
		c.ConsumerTopics,
		c.ClusterConfig,
	)

	c.Logger.Info(
		"Using provided Kafka consumer configuration",
		zap.Strings("brokers", c.ConsumerBrokers),
		zap.String("group", c.ConsumerGroup),
		zap.Strings("topics", c.ConsumerTopics),
	)

	if err != nil {
		panic(err)
	}

	consumer := &Consumer{
		cc:       kafkaconsumer,
		messages: make(chan *stream.Message),
	}

	go func() {
		defer kafkaconsumer.Close()
		var message stream.Message

		for msg := range kafkaconsumer.Messages() {
			message = stream.Message{
				Value:     msg.Value,
				Timestamp: msg.Timestamp,
			}

			consumer.messages <- &message
		}
	}()

	go func() {
		for err := range kafkaconsumer.Errors() {
			c.Logger.Error("Kafka consumer received error.", zap.Error(err))
		}
	}()

	go func() {
		for note := range kafkaconsumer.Notifications() {
			c.Logger.Info("Kafka consumer received notification.", zap.Any("notification", note))
		}
	}()

	return consumer
}

// Consumer implements the stream.Consumer interface for standardstream.
type Consumer struct {
	cc       *cluster.Consumer
	messages chan *stream.Message
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
}
