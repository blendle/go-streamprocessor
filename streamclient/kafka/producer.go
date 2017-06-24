package kafka

import (
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
)

// NewProducer returns a consumer that can iterate over messages on a stream.
func (c *Client) NewProducer() stream.Producer {
	var err error

	ch := make(chan *stream.Message)
	producer := &Producer{messages: ch}
	producer.sp, err = sarama.NewAsyncProducer(c.Brokers, nil)
	if err != nil {
		panic(err)
	}

	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()

		for msg := range ch {
			message := sarama.ProducerMessage{
				Timestamp: msg.Timestamp,
				Topic:     c.Topics[0],
				Value:     sarama.ByteEncoder(msg.Value),
			}

			if key := producer.keyFunc(msg); key != nil {
				message.Key = sarama.ByteEncoder(producer.keyFunc(msg))
			}

			producer.sp.Input() <- &message
		}
	}()

	go func() {
		for m := range producer.sp.Successes() {
			log.Printf("%v", m)
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
	keyFunc  func(*stream.Message) []byte
	messages chan<- *stream.Message
	sp       sarama.AsyncProducer
	wg       sync.WaitGroup
}

// Messages returns the read channel for the messages that are returned by the
// stream.
func (p *Producer) Messages() chan<- *stream.Message {
	return p.messages
}

// Close closes the producer connection.
func (p *Producer) Close() error {
	close(p.messages)
	p.wg.Wait()

	return p.sp.Close()
}

// PartitionKey can be used to define the key to use for partitioning messages.
func (p *Producer) PartitionKey(f func(*stream.Message) []byte) {
	p.keyFunc = f
}
