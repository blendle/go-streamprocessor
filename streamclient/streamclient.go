package streamclient

import (
	"os"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmem"
	"github.com/blendle/go-streamprocessor/streamclient/kafka"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	cluster "github.com/bsm/sarama-cluster"
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

// NewConsumer returns a new consumer, based on the environment in which the
// function is called.
//
// If data is being piped from Stdin, the consumer client will be
// standardstream. Otherwise it will use the Kafka client.
func NewConsumer(options ...func(sc *standardstream.Client, kc *kafka.Client)) (stream.Consumer, error) {
	var consumer stream.Consumer

	sc := &standardstream.Client{}
	kc := &kafka.Client{}
	kc.ConsumerConfig = cluster.NewConfig()
	kc.ProducerConfig = sarama.NewConfig()

	for _, option := range options {
		option(sc, kc)
	}

	if sc.ConsumerFD == nil {
		sc.ConsumerFD = os.Stdin
	}

	scopt := func(c *standardstream.Client) {
		c.ConsumerFD = sc.ConsumerFD
		c.Logger = sc.Logger
	}

	kcopt := func(c *kafka.Client) {
		c.ConsumerBrokers = kc.ConsumerBrokers
		c.ConsumerGroup = kc.ConsumerGroup
		c.ConsumerTopics = kc.ConsumerTopics
		c.Logger = kc.Logger
	}

	// Check for any data on the provided file descriptor. If there is any, use
	// the standardstream client for the consumer.
	//
	// If the fd contains no data, use the default Kafka client.
	stat, err := sc.ConsumerFD.Stat()
	if err != nil {
		return consumer, err
	}

	if stat.Size() > 0 {
		c := standardstream.NewClient(scopt)
		consumer = c.NewConsumer()
	} else {
		c := kafka.NewClient(kcopt)
		consumer = c.NewConsumer()
	}

	return consumer, nil
}

// NewProducer returns a new producer, based on the environment in which the
// function is called.
//
// The producer will be of the Kafka client by default, unless the `DRY_RUN`
// environment variable is defined, in which case it will be from the
// standardstream client.
func NewProducer(options ...func(sc *standardstream.Client, kc *kafka.Client)) (stream.Producer, error) {
	var producer stream.Producer

	sc := &standardstream.Client{}
	kc := &kafka.Client{}
	kc.ConsumerConfig = cluster.NewConfig()
	kc.ProducerConfig = sarama.NewConfig()

	for _, option := range options {
		option(sc, kc)
	}

	scopt := func(c *standardstream.Client) {
		c.ProducerFD = sc.ProducerFD
		c.Logger = sc.Logger
	}

	kcopt := func(c *kafka.Client) {
		c.ProducerBrokers = kc.ProducerBrokers
		c.ProducerTopics = kc.ProducerTopics
		c.Logger = kc.Logger
	}

	// if the `DRY_RUN` environment variable is defined, use the standardstream
	// client for the producer.
	//
	// If not, use the Kafka client.
	if os.Getenv("DRY_RUN") != "" {
		c := standardstream.NewClient(scopt)
		producer = c.NewProducer()
	} else {
		c := kafka.NewClient(kcopt)
		producer = c.NewProducer()
	}

	return producer, nil
}

// NewConsumerAndProducer returns the values of `NewConsumer` and `NewProducer`,
// or an error if any of the two returns an error.
//
// See the two function descriptions for more details.
func NewConsumerAndProducer(options ...func(sc *standardstream.Client, kc *kafka.Client)) (stream.Consumer, stream.Producer, error) {
	var err error
	var consumer stream.Consumer
	var producer stream.Producer

	consumer, err = NewConsumer(options...)
	if err != nil {
		return consumer, producer, err
	}

	producer, err = NewProducer(options...)
	return consumer, producer, err
}
