package kafkaclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils/testutils"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationTestConsumer(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)

	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup)
	defer closer()

	assert.Equal(t, "*kafkaclient.Consumer", reflect.TypeOf(consumer).String())
	assert.Equal(t, topicAndGroup, consumer.Config().Kafka.Topics[0])
}

func TestIntegrationTestConsumer_WithOptions(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)
	options := func(c *streamconfig.Consumer) {
		c.Kafka.ID = "TestTestConsumer_WithOptions"
	}

	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup, options)
	defer closer()

	assert.Equal(t, "TestTestConsumer_WithOptions", consumer.Config().Kafka.ID)
	assert.Equal(t, topicAndGroup, consumer.Config().Kafka.Topics[0])
}

func TestIntegrationTestProducer(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topic := testutils.Random(t)

	producer, closer := kafkaclient.TestProducer(t, topic)
	defer closer()

	assert.Equal(t, "*kafkaclient.Producer", reflect.TypeOf(producer).String())
}

func TestIntegrationTestProducer_WithOptions(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topic := testutils.Random(t)
	options := func(c *streamconfig.Producer) {
		c.Kafka.ID = "TestTestProducer_WithOptions"
	}

	producer, closer := kafkaclient.TestProducer(t, topic, options)
	defer closer()

	assert.Equal(t, "TestTestProducer_WithOptions", producer.Config().Kafka.ID)
	assert.Equal(t, topic, producer.Config().Kafka.Topic)
}

func TestIntegrationTestMessageFromConsumer(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list":  kafkaclient.TestBrokerAddress,
		"produce.offset.report": false,
	}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}

	require.NoError(t, producer.Produce(msg, nil))
	<-producer.Events()
	require.Zero(t, producer.Flush(1000))
	producer.Close()

	options := func(c *streamconfig.Consumer) {
		c.Kafka.Brokers = []string{kafkaclient.TestBrokerAddress}
		c.Kafka.Topics = []string{topicAndGroup}
		c.Kafka.GroupID = topicAndGroup
	}

	consumer, err := kafkaclient.NewConsumer(options)
	require.NoError(t, err)
	defer func() { assert.NoError(t, consumer.Close()) }()

	message := kafkaclient.TestMessageFromConsumer(t, consumer)

	assert.Equal(t, "hello world", string(message.Value))
}

func TestIntegrationTestMessageFromTopic(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list":  kafkaclient.TestBrokerAddress,
		"produce.offset.report": false,
	}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}

	require.NoError(t, producer.Produce(msg, nil))
	<-producer.Events()
	require.Zero(t, producer.Flush(1000))
	producer.Close()

	options := func(c *streamconfig.Consumer) {
		c.Kafka.Brokers = []string{kafkaclient.TestBrokerAddress}
		c.Kafka.Topics = []string{topicAndGroup}
		c.Kafka.GroupID = topicAndGroup
	}

	consumer, err := kafkaclient.NewConsumer(options)
	require.NoError(t, err)
	defer func() { assert.NoError(t, consumer.Close()) }()

	message := kafkaclient.TestMessageFromConsumer(t, consumer)

	assert.Equal(t, "hello world", string(message.Value))
}

func TestIntegrationTestMessagesFromTopic(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list":  kafkaclient.TestBrokerAddress,
		"produce.offset.report": false,
	}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg1 := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}
	require.NoError(t, producer.Produce(msg1, nil))
	<-producer.Events()

	msg2 := &kafka.Message{Value: []byte("hello universe!"), TopicPartition: p}
	require.NoError(t, producer.Produce(msg2, nil))
	<-producer.Events()

	require.Zero(t, producer.Flush(1000))
	producer.Close()

	messages := kafkaclient.TestMessagesFromTopic(t, topicAndGroup)

	require.Len(t, messages, 2)
	assert.Equal(t, "hello world", string(messages[0].Value))
	assert.Equal(t, "hello universe!", string(messages[1].Value))
}

func TestIntegrationTestProduceMessages(t *testing.T) {
	testutils.Integration(t)

	var tests = map[string]struct {
		ifaces []interface{}
		values []string
	}{
		"string": {
			[]interface{}{"hello world"},
			[]string{"hello world"},
		},

		"multiple": {
			[]interface{}{"hello world", "hello universe!", "COSMOS!"},
			[]string{"hello world", "hello universe!", "COSMOS!"},
		},

		"kv": {
			[]interface{}{[]string{"key1", "hello world"}, []string{"key2", "hello universe!"}},
			[]string{"hello world", "hello universe!"},
		},

		"streammsg.Message": {
			[]interface{}{streammsg.Message{Value: []byte("hello world")}},
			[]string{"hello world"},
		},

		"kafka.Message": {
			[]interface{}{kafka.Message{Value: []byte("hello world")}},
			[]string{"hello world"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			topicAndGroup := testutils.Random(t)
			println(topicAndGroup)

			config := &kafka.ConfigMap{
				"metadata.broker.list":     kafkaclient.TestBrokerAddress,
				"group.id":                 topicAndGroup,
				"enable.partition.eof":     true,
				"go.events.channel.enable": true,
				"default.topic.config":     kafka.ConfigMap{"auto.offset.reset": "beginning"},
			}

			consumer, err := kafka.NewConsumer(config)
			require.NoError(t, err)
			kafkaclient.TestProduceMessages(t, topicAndGroup, tt.ifaces...)

			require.NoError(t, consumer.SubscribeTopics([]string{topicAndGroup}, nil))

			var messages []*kafka.Message
			run := true
			for run {
				event := <-consumer.Events()
				switch e := event.(type) {
				case *kafka.Message:
					messages = append(messages, e)
				case kafka.PartitionEOF:
					run = false
				}
			}

			require.Len(t, messages, len(tt.values))
			for i, v := range tt.values {
				assert.Equal(t, messages[i].Value, []byte(v))
			}

			assert.NoError(t, consumer.Close())
		})
	}
}

func TestIntegrationTestOffsets(t *testing.T) {
	t.Parallel()
	testutils.Integration(t)

	topicAndGroup := testutils.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list":  kafkaclient.TestBrokerAddress,
		"produce.offset.report": false,
	}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}
	require.NoError(t, producer.Produce(msg, nil))
	<-producer.Events()
	producer.Close()

	options := func(c *streamconfig.Consumer) {
		c.Kafka.Brokers = []string{kafkaclient.TestBrokerAddress}
		c.Kafka.Topics = []string{topicAndGroup}
		c.Kafka.GroupID = topicAndGroup
	}

	consumer, err := kafkaclient.NewConsumer(options)
	require.NoError(t, err)
	defer func() { assert.NoError(t, consumer.Close()) }()

	message := <-consumer.Messages()
	tp := kafkaclient.TestOffsets(t, message)
	assert.Equal(t, kafka.Offset(-1001), tp[0].Offset)

	require.NoError(t, consumer.Ack(message))
	require.NoError(t, consumer.Close())

	tp = kafkaclient.TestOffsets(t, message)
	assert.Equal(t, kafka.Offset(1), tp[0].Offset)
}

func TestIntegrationTestConsumerConfig(t *testing.T) {
	t.Parallel()

	topicAndGroup := testutils.Random(t)
	config := kafkaclient.TestConsumerConfig(t, topicAndGroup)

	size := 1
	if testing.Verbose() {
		size = 2
	}

	assert.Len(t, config, size)
}

func TestIntegrationTestProducerConfig(t *testing.T) {
	t.Parallel()

	topicAndGroup := testutils.Random(t)
	config := kafkaclient.TestProducerConfig(t, topicAndGroup)

	size := 1
	if testing.Verbose() {
		size = 2
	}

	assert.Len(t, config, size)
}
