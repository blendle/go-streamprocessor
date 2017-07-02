package streamclient

import (
	"os"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmem"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
)

// Client is the overarching configuration for all stream clients.
type Client struct {
	Standardstream *standardstream.Client
	Kafka          *kafka.Client
	Inmem          *inmem.Client
}

// NewStandardStreamClient returns a new standardstream client.
func NewStandardStreamClient(options ...func(*standardstream.Client)) stream.Client {
	return standardstream.NewClient(options...)
}

// NewKafkaClient returns a new Kafka client.
func NewKafkaClient(options ...func(*kafka.Client)) stream.Client {
	return kafka.NewClient(options...)
}

// NewInmemClient returns a new inmem client.
func NewInmemClient(options ...func(*inmem.Client)) stream.Client {
	return inmem.NewClient(options...)
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
func NewConsumerAndProducer(options ...func(sc *standardstream.Client, kc *kafka.Client)) (stream.Consumer, stream.Producer) {
	var k stream.Client
	var s stream.Client

	var c stream.Consumer
	var p stream.Producer

	sc := &standardstream.Client{}
	kc := &kafka.Client{}

	for _, option := range options {
		option(sc, kc)
	}

	if sc.ConsumerFD == nil {
		sc.ConsumerFD = os.Stdin
	}

	scopt := func(c *standardstream.Client) {
		c.ConsumerFD = sc.ConsumerFD
		c.ProducerFD = sc.ProducerFD
		c.Logger = sc.Logger
	}

	kcopt := func(c *kafka.Client) {
		c.ConsumerBrokers = kc.ConsumerBrokers
		c.ConsumerGroup = kc.ConsumerGroup
		c.ConsumerTopics = kc.ConsumerTopics
		c.ProducerBrokers = kc.ProducerBrokers
		c.ProducerTopics = kc.ProducerTopics
		c.Logger = kc.Logger
	}

	// Check for any data on the provided file descriptor. If there is any, use
	// the standardstream client for the consumer.
	//
	// If the fd contains no data, use the default Kafka client.
	stat, err := sc.ConsumerFD.Stat()
	if err != nil {
		panic(err)
	}

	if stat.Size() > 0 {
		s = standardstream.NewClient(scopt)
		c = s.NewConsumer()
	} else {
		k = kafka.NewClient(kcopt)
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
			s = standardstream.NewClient(scopt)
		}

		p = s.NewProducer()
	} else {
		if k == nil {
			k = kafka.NewClient(kcopt)
		}

		p = k.NewProducer()
	}

	return c, p
}
