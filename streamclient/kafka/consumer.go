package kafka

import (
	"github.com/blendle/go-streamprocessor/stream"
	cluster "github.com/bsm/sarama-cluster"
)

// NewConsumer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewConsumer() stream.Consumer {
	kafkaconsumer, err := cluster.NewConsumer(
		c.ConsumerBrokers,
		c.ConsumerGroup,
		c.ConsumerTopics,
		c.ClusterConfig,
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
