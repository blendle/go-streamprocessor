package main

import (
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
)

func main() {
	// Instantiate a new consumer and producer.
	consumer, producer, err := streamclient.NewConsumerAndProducer()
	if err != nil {
		panic(err)
	}

	// Don't forget to close the producer once you're done.
	defer producer.Close()

	for msg := range consumer.Messages() {
		// Initialize a new message.
		message := &stream.Message{}

		// Set the value of the message to the consumed message, prepended with
		// "processed: ".
		message.Value = append([]byte("processed: "), msg.Value...)

		// Send message to the configured producer.
		producer.Messages() <- message

		// Mark the consumed message as "done". This lets the client know it should
		// not retry this message if the application is restarted.
		msg.Done()
	}
}
