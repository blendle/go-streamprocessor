package kafka

import (
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
	cluster "github.com/bsm/sarama-cluster"
	"go.uber.org/zap"
)

// Client provides access to the streaming capabilities.
type Client struct {
	ConsumerConfig  *cluster.Config
	ConsumerBrokers []string
	ConsumerTopics  []string
	ConsumerGroup   string

	ProducerConfig  *sarama.Config
	ProducerBrokers []string
	ProducerTopics  []string

	// Logger is the configurable logger instance to log messages from this
	// streamclient. If left undefined, a noop logger will be used.
	Logger *zap.Logger
}

// NewClient returns a new kafka client.
func NewClient(options ...func(*Client)) stream.Client {
	c := &Client{}

	c.ConsumerConfig = cluster.NewConfig()
	c.ConsumerConfig.Consumer.Fetch.Default = 1048576
	c.ConsumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.ConsumerConfig.Consumer.Retry.Backoff = time.Duration(1) * time.Second
	c.ConsumerConfig.Consumer.Return.Errors = true
	c.ConsumerConfig.Group.Return.Notifications = true
	c.ConsumerConfig.Metadata.RefreshFrequency = 5 * time.Minute
	c.ConsumerConfig.Net.KeepAlive = 5 * time.Minute
	c.ConsumerConfig.Net.MaxOpenRequests = 64
	c.ConsumerConfig.Version = sarama.V0_10_2_0

	c.ProducerConfig = sarama.NewConfig()
	c.ProducerConfig.Metadata.RefreshFrequency = 5 * time.Minute
	c.ProducerConfig.Net.KeepAlive = 5 * time.Minute
	c.ProducerConfig.Net.MaxOpenRequests = 64
	c.ProducerConfig.Producer.Compression = sarama.CompressionSnappy
	c.ProducerConfig.Producer.Retry.Max = 10
	c.ProducerConfig.Producer.Return.Errors = true
	c.ProducerConfig.Version = sarama.V0_10_2_0

	for _, option := range options {
		option(c)
	}

	if len(c.ConsumerBrokers) == 0 || len(c.ProducerBrokers) == 0 {
		c.autoPopulateKafkaConfig()
	}

	if c.Logger == nil {
		c.Logger = zap.NewNop()
	}

	return c
}

// NewConsumerAndProducer is a convenience method that returns both a consumer
// and a producer, with a single function call.
func (c *Client) NewConsumerAndProducer() (stream.Consumer, stream.Producer) {
	return c.NewConsumer(), c.NewProducer()
}

func (c *Client) autoPopulateKafkaConfig() {
	var broker, group, topic string
	var err error

	if len(c.ConsumerBrokers) == 0 {
		if u := os.Getenv("KAFKA_CONSUMER_URL"); u != "" {
			broker, group, topic, err = ParseKafkaURL(u)
			if err != nil {
				return
			}
		} else {
			broker = "localhost:9092"
			group = "test-group"
			topic = "consumer-topic"
		}

		c.ConsumerBrokers = []string{broker}
		c.ConsumerTopics = []string{topic}
		c.ConsumerGroup = group
	}

	if len(c.ProducerBrokers) == 0 {
		if u := os.Getenv("KAFKA_PRODUCER_URL"); u != "" {
			broker, _, topic, err = ParseKafkaURL(u)
			if err != nil {
				return
			}
		} else {
			broker = "localhost:9092"
			topic = "producer-topic"
		}

		c.ProducerBrokers = []string{broker}
		c.ProducerTopics = []string{topic}
	}
}

// ParseKafkaURL parses a Kafka URL and returns the relevant components.
func ParseKafkaURL(uri string) (string, string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", "", err
	}

	broker := u.Host
	group := u.Query().Get("group")
	topic := strings.TrimPrefix(u.Path, "/")

	return broker, group, topic, nil
}
