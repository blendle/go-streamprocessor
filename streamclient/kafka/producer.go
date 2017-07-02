package kafka

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
)

// NewProducer returns a producer that produces messages on a Kafka stream.
func (c *Client) NewProducer() stream.Producer {
	var err error

	ch := make(chan *stream.Message)
	producer := &Producer{messages: ch}
	producer.sp, err = sarama.NewAsyncProducer(c.ProducerBrokers, nil)
	if err != nil {
		panic(err)
	}

	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()
		for msg := range ch {
			message := sarama.ProducerMessage{
				Timestamp: msg.Timestamp,
				Topic:     c.ProducerTopics[0],
				Value:     sarama.ByteEncoder(msg.Value),
			}

			if producer.keyFunc != nil {
				key := producer.keyFunc(msg)
				if key != nil {
					message.Key = sarama.ByteEncoder(key)
				}
			}

			producer.sp.Input() <- &message
		}
	}()

	go func() {
		for err := range producer.sp.Errors() {
			log.Println(err)
		}
	}()

	return producer
}

// Producer represents the object that will produce messages to a stream.
type Producer struct {
	// The function used to determine the partition key of a message. If left
	// undefined, no partitioning will happen, and the messages are evenly
	// distributed across all partitions.
	keyFunc func(*stream.Message) []byte

	// This channel is used by the user to send messages to the producer.
	// Internally, the message is converted to a `sarama.ProducerMessage` message
	// and then passed on to the AsyncProducer channel.
	messages chan<- *stream.Message

	// The `sarama.AsyncProducer` value is used to be able to call `Close` on the
	// producer at a later point in time.
	sp sarama.AsyncProducer

	// The `sync.WaitGroup` is used to wait for all messages to be processed, when
	// the user calls `Producer.Close()`.
	wg sync.WaitGroup
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (p *Producer) Messages() chan<- *stream.Message {
	return p.messages
}

// Close closes the producer connection.
func (p *Producer) Close() error {
	// Close the producer channel, rejecting any future messages, but continue to
	// process any messages still in the buffer.
	close(p.messages)

	// Wait for all messages to be committed to the AsyncProducer, or else we
	// might trigger a `sarama.ErrShuttingDown` error, causing one or more
	// messages to be lost.
	p.wg.Wait()

	// Shut down the Sarama AsyncProducer, which will block until all messages are
	// processed, before shutting down.
	return p.sp.Close()
}

// PartitionKey can be used to define the key to use for partitioning messages.
//
// You pass in a function that accepts the currently processed message as its
// single value and should return byte slice, representing the partition key.
//
// You can either set the key to a fixed byte slice, or determine the partition
// key based on the message's value or other properties.
func (p *Producer) PartitionKey(f func(*stream.Message) []byte) {
	p.keyFunc = f
}
