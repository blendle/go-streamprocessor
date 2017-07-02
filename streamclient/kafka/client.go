package kafka

import (
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
	cluster "github.com/bsm/sarama-cluster"
)

// Client provides access to the streaming capabilities.
type Client struct {
	ClusterConfig *cluster.Config
	SaramaConfig  *sarama.Config

	ConsumerBrokers []string
	ConsumerTopics  []string
	ConsumerGroup   string

	ProducerBrokers []string
	ProducerTopics  []string
}

// NewClient returns a new kafka client.
func NewClient(options ...func(*Client)) stream.Client {
	c := &Client{}

	if len(c.ConsumerBrokers) == 0 || len(c.ProducerBrokers) == 0 {
		c.autoPopulateKafkaConfig()
	}

	c.ClusterConfig = cluster.NewConfig()
	c.ClusterConfig.Consumer.Return.Errors = true
	c.ClusterConfig.Group.Return.Notifications = true
	c.ClusterConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.ClusterConfig.Version = sarama.V0_10_0_1

	c.SaramaConfig = sarama.NewConfig()
	c.SaramaConfig.Net.MaxOpenRequests = 64
	c.SaramaConfig.Net.KeepAlive = 5 * time.Minute
	c.SaramaConfig.Version = sarama.V0_10_0_1
	c.SaramaConfig.Producer.Return.Errors = true
	c.SaramaConfig.Producer.Retry.Max = 10

	for _, option := range options {
		option(c)
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
