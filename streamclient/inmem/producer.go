package inmem

import (
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
)

// NewProducer returns a producer that outputs messages to stdout.
func (c *Client) NewProducer() stream.Producer {
	ch := make(chan *stream.Message)
	producer := &Producer{messages: ch}

	producer.PartitionKey(func(_ *stream.Message) []byte {
		return nil
	})

	producer.wg.Add(1)
	go func() {
		defer producer.wg.Done()
		for msg := range ch {
			key := msg.Key

			if producer.keyFunc != nil {
				key = producer.keyFunc(msg)
			}

			c.store.NewTopic(c.topicName(msg)).NewMessage(msg.Value, key)
		}
	}()

	return producer
}

// Producer represents the object that will produce messages to a stream.
type Producer struct {
	wg       sync.WaitGroup
	messages chan<- *stream.Message
	keyFunc  func(*stream.Message) []byte
}

// Messages returns the write channel for messages to be produced.
func (p *Producer) Messages() chan<- *stream.Message {
	return p.messages
}

// Close closes the producer connection
func (p *Producer) Close() error {
	close(p.messages)
	p.wg.Wait()

	return nil
}

// PartitionKey can be used to define the key to use for partitioning messages.
func (p *Producer) PartitionKey(f func(*stream.Message) []byte) {
	p.keyFunc = f
}
