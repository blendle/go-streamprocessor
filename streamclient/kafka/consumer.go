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
		c.ConsumerConfig,
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
		closed:   make(chan bool),
		close:    make(chan bool),
	}

	go func() {
		defer kafkaconsumer.Close()
		for {
			select {
			case msg := <-kafkaconsumer.Messages():
				message := stream.NewMessageFromKafka(msg, kafkaconsumer)
				select {
				case consumer.messages <- message:
				case <-consumer.close:
					consumer.closed <- true
					return
				}
			case <-consumer.close:
				consumer.closed <- true
				return
			}
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
	close    chan bool
	closed   chan bool
	dead     bool
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (c *Consumer) Messages() <-chan *stream.Message {
	return c.messages
}

// Close closes the consumer connection.
func (c *Consumer) Close() error {
	if c.dead {
		return nil
	}

	c.close <- true

	// Shut down the Sarama Consumer, which will block until all messages are
	// processed, before shutting down.
	err := c.cc.Close()
	if err != nil {
		return err
	}

	<-c.closed

	// Close the consumer channel.
	close(c.messages)

	c.dead = true

	return nil
}
