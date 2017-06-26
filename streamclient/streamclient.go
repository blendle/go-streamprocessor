package streamclient

import (
	"os"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
)

// NewStandardStreamClient returns a new standardstream client.
func NewStandardStreamClient() stream.Client {
	return standardstream.NewClient(&standardstream.ClientConfig{})
}

// NewKafkaClient returns a new Kafka client.
func NewKafkaClient() stream.Client {
	return kafka.NewClient()
}

// NewConsumerAndProducer returns the a consumer and producer, based on the
// environment in which the function is called.
//
// If data is being piped from Stdin, the consumer client will be
// standardstream. Otherwise it will use the Kafka client.
//
// The producer will be of the Kafka client by default, unless the `DRY_RUN`
// environment variable is defined, in which case it will be from the
// standardstream client.
func NewConsumerAndProducer(ssc *standardstream.ClientConfig) (stream.Consumer, stream.Producer) {
	var k stream.Client
	var s stream.Client

	var c stream.Consumer
	var p stream.Producer

	// Set the file descriptor to `os.Stdin` by default, unless another fd is
	// already provided.
	if ssc.ConsumerFD == nil {
		ssc.ConsumerFD = os.Stdin
	}

	// Check for any data on the provided file descriptor. If there is any, use
	// the standardstream client for the consumer.
	//
	// If the fd contains no data, use the default Kafka client.
	stat, _ := ssc.ConsumerFD.Stat()
	if stat.Size() > 0 {
		s = standardstream.NewClient(ssc)
		c = s.NewConsumer()
	} else {
		k = kafka.NewClient()
		c = k.NewConsumer()
	}

	// if the `DRY_RUN` environment variable is defined, use the standardstream
	// client for the producer.
	//
	// If not, use the Kafka client.
	//
	// Tries to reuse an existing client, if it exists.
	if os.Getenv("DRY_RUN") != "" {
		if s == nil {
			s = standardstream.NewClient(ssc)
		}

		p = s.NewProducer()
	} else {
		if k == nil {
			k = kafka.NewClient()
		}

		p = k.NewProducer()
	}

	return c, p
}
