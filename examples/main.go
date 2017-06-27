package main

import (
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
)

func main() {
	consumer, producer := streamclient.NewConsumerAndProducer(&standardstream.ClientConfig{})
	defer producer.Close()

	for msg := range consumer.Messages() {
		message := &stream.Message{}

		message.Value = append([]byte("processed: "), msg.Value...)

		producer.Messages() <- message
	}
}
