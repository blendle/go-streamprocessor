package main

import (
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
)

func main() {
	consumer, producer := streamclient.NewConsumerAndProducer()
	defer producer.Close()

	for msg := range consumer.Messages() {
		message := &stream.Message{}

		message.Value = append([]byte("processed: "), msg.Value...)

		producer.Messages() <- message
	}
}
