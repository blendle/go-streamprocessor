package kafkaclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/v3/streamutil/testutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationTestConsumer(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup)
	defer closer()

	assert.Equal(t, "*kafkaclient.consumer", reflect.TypeOf(consumer).String())
	assert.Equal(t, topicAndGroup, consumer.Config().(streamconfig.Consumer).Kafka.Topics[0])
}

func TestIntegrationTestConsumer_WithOptions(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	id := streamconfig.KafkaID("TestTestConsumer_WithOptions")
	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup, id)
	defer closer()

	cfg := consumer.Config().(streamconfig.Consumer)
	assert.Equal(t, "TestTestConsumer_WithOptions", cfg.Kafka.ID)
	assert.Equal(t, topicAndGroup, cfg.Kafka.Topics[0])
}

func TestIntegrationTestProducer(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)

	producer, closer := kafkaclient.TestProducer(t, topic)
	defer closer()

	assert.Equal(t, "*kafkaclient.producer", reflect.TypeOf(producer).String())
}

func TestIntegrationTestProducer_WithOptions(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)

	id := streamconfig.KafkaID("TestTestProducer_WithOptions")
	producer, closer := kafkaclient.TestProducer(t, topic, id)
	defer closer()

	cfg := producer.Config().(streamconfig.Producer)
	assert.Equal(t, "TestTestProducer_WithOptions", cfg.Kafka.ID)
	assert.Equal(t, topic, cfg.Kafka.Topic)
}

func TestIntegrationTestMessageFromTopic(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list": kafkaconfig.TestBrokerAddress,
	}

	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}

	require.NoError(t, producer.Produce(msg, nil))
	<-producer.Events()
	require.Zero(t, producer.Flush(1000))
	producer.Close()

	message := kafkaclient.TestMessageFromTopic(t, topicAndGroup)

	assert.Equal(t, "hello world", string(message.Value))
}

func TestIntegrationTestMessagesFromTopic(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list": kafkaconfig.TestBrokerAddress,
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
	t.Parallel()
	testutil.Integration(t)

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

		"stream.Message": {
			[]interface{}{stream.Message{Value: []byte("hello world")}},
			[]string{"hello world"},
		},

		"kafka.Message": {
			[]interface{}{kafka.Message{Value: []byte("hello world")}},
			[]string{"hello world"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			topicAndGroup := testutil.Random(t)

			config := &kafka.ConfigMap{
				"metadata.broker.list":     kafkaconfig.TestBrokerAddress,
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
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	config := &kafka.ConfigMap{
		"metadata.broker.list": kafkaconfig.TestBrokerAddress,
		"message.timeout.ms":   60000,
	}
	producer, err := kafka.NewProducer(config)
	require.NoError(t, err)

	p := kafka.TopicPartition{Topic: &topicAndGroup, Partition: kafka.PartitionAny} // nolint: gotypex
	msg := &kafka.Message{Value: []byte("hello world"), TopicPartition: p}
	require.NoError(t, producer.Produce(msg, nil))
	<-producer.Events()
	producer.Close()

	consumer, err := kafkaclient.NewConsumer(
		streamconfig.KafkaBroker(kafkaconfig.TestBrokerAddress),
		streamconfig.KafkaTopic(topicAndGroup),
		streamconfig.KafkaGroupID(topicAndGroup),
	)
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

func TestTestConsumerConfig(t *testing.T) {
	t.Parallel()

	topicAndGroup := testutil.Random(t)
	config := kafkaclient.TestConsumerConfig(t, topicAndGroup)

	size := 1
	if testutil.Verbose(t) {
		size = 2
	}

	assert.Len(t, config, size)
}

func TestTestProducerConfig(t *testing.T) {
	t.Parallel()

	topicAndGroup := testutil.Random(t)
	config := kafkaclient.TestProducerConfig(t, topicAndGroup)

	size := 1
	if testutil.Verbose(t) {
		size = 2
	}

	assert.Len(t, config, size)
}
