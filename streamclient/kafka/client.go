package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/blendle/go-streamprocessor/stream"
	cluster "github.com/bsm/sarama-cluster"
	metrics "github.com/rcrowley/go-metrics"
)

// Client provides access to the streaming capabilities.
type Client struct {
	ClusterConfig *cluster.Config
	SaramaConfig  *sarama.Config
	Brokers       []string
	Topics        []string
	ConsumerGroup string
}

// NewClient returns a new kafka client.
func NewClient() stream.Client {
	c := &Client{}

	c.ClusterConfig = cluster.NewConfig()
	c.ClusterConfig.Consumer.Return.Errors = true
	c.ClusterConfig.Group.Return.Notifications = true
	c.ClusterConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	c.ClusterConfig.Version = sarama.V0_10_2_0

	metrics.UseNilMetrics = true

	c.SaramaConfig = sarama.NewConfig()
	c.SaramaConfig.Net.MaxOpenRequests = 64
	c.SaramaConfig.Net.KeepAlive = 5 * time.Minute
	c.SaramaConfig.Version = sarama.V0_10_2_0
	c.SaramaConfig.Producer.Return.Errors = true
	c.SaramaConfig.Producer.Retry.Max = 10

	c.Brokers = []string{"127.0.0.1:9092"}
	c.Topics = []string{"test-topic"}
	c.ConsumerGroup = "test-consumer-group"

	return c
}

// NewConsumerAndProducer is a convenience method that returns both a consumer
// and a producer, with a single function call.
func (c *Client) NewConsumerAndProducer() (stream.Consumer, stream.Producer) {
	return c.NewConsumer(), c.NewProducer()
}
