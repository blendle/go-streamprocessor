package kafkaclient

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestConsumer returns a new kafka consumer to be used in test cases. It also
// returns a function that should be deferred to clean up resources.
//
// You pass the topic and group name of the consumer as a single argument.
func TestConsumer(tb testing.TB, topicAndGroup string, options ...streamconfig.Option) (stream.Consumer, func()) {
	tb.Helper()

	consumer, err := NewConsumer(TestConsumerConfig(tb, topicAndGroup, options...)...)
	require.NoError(tb, err)

	return consumer, func() { require.NoError(tb, consumer.Close()) }
}

// TestConsumerWithAssignments is the same as `TestConsumer`, except that it
// waits for topic assignments to finish. If no topic assignment happens within
// a hard-coded period of 5 seconds, an error is triggered.
func TestConsumerWithAssignments(tb testing.TB, topicAndGroup string, options ...streamconfig.Option) (stream.Consumer, func()) {
	c, closer := TestConsumer(tb, topicAndGroup, options...)

	kc := c.(*consumer)
	for i := 0; i < 50; i++ {
		tp, err := kc.kafka.Assignment()
		require.NoError(tb, err)

		if len(tp) > 0 {
			return c, closer
		}

		time.Sleep(100 * time.Millisecond)
	}

	tb.Fatal("timeout while waiting for topic assignment")

	return nil, nil
}

// TestProducer returns a new Kafka consumer to be used in test cases. It also
// returns a function that should be deferred to clean up resources.
//
// You pass the topic and group name of the consumer as a single argument.
func TestProducer(tb testing.TB, topic string, options ...streamconfig.Option) (stream.Producer, func()) {
	tb.Helper()

	producer, err := NewProducer(TestProducerConfig(tb, topic, options...)...)
	require.NoError(tb, err)

	return producer, func() { require.NoError(tb, producer.Close()) }
}

// TestMessageFromTopic returns a single message, consumed from the provided
// topic. It has a built-in timeout mechanism to prevent the test from getting
// stuck.
func TestMessageFromTopic(tb testing.TB, topic string) stream.Message {
	tb.Helper()

	consumer, closer := testKafkaConsumer(tb, topic, false)
	defer closer()

	m, err := consumer.ReadMessage(testutil.MultipliedDuration(tb, 3*time.Second))
	require.NoError(tb, err)

	return *newMessageFromKafka(m)
}

// TestMessagesFromTopic returns all messages in a topic.
func TestMessagesFromTopic(tb testing.TB, topic string) []stream.Message {
	tb.Helper()

	consumer, closer := testKafkaConsumer(tb, topic, true)
	defer closer()

	var messages []stream.Message
	for event := range consumer.Events() {
		switch e := event.(type) {
		case *kafka.Message:
			messages = append(messages, *newMessageFromKafka(e))
		case kafka.PartitionEOF:
			return messages
		}
	}

	return messages
}

// TestProduceMessages accepts a string to use as the topic, and an arbitrary
// number of argument to generate messages on the provided Kafka topic.
//
// The provided extra arguments can be of several different types:
//
// * `string` – The value is used as the kafka message value.
//
// * `[]string` – The first value is used as the kafka message key, the second
// as the message value, all other values are ignored.
//
// * `stream.Message` – The value (and, if applicable, the key) are set on a new
// `kafka.Message`.
//
// * `*kafka.Message` – The message is delivered to Kafka as-is. If
// `kafka.TopicPartition` is empty, the passed in topic value is used instead.
func TestProduceMessages(tb testing.TB, topic string, values ...interface{}) {
	tb.Helper()

	producer, closer := testKafkaProducer(tb)
	defer closer()

	tp := kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny} // nolint: gotype
	for _, v := range values {
		m := kafka.Message{TopicPartition: tp}

		switch value := v.(type) {
		case string:
			m.Value = []byte(value)
		case []string:
			require.Len(tb, value, 2)

			m.Key = []byte(value[0])
			m.Value = []byte(value[1])
		case stream.Message:
			m.Value = value.Value
			m.Key = value.Key
		case kafka.Message:
			m = value
			if m.TopicPartition == (kafka.TopicPartition{}) {
				m.TopicPartition = tp
			}
		default:
			require.Fail(tb, "invalid interface type received.", "type: %s", reflect.TypeOf(value).String())
		}

		require.NoError(tb, producer.Produce(&m, nil))

		select {
		case <-producer.Events():
		case <-time.After(testutil.MultipliedDuration(tb, 5*time.Second)):
			require.Fail(tb, "Timeout while waiting for message to be delivered.")
		}
	}
}

// TestOffsets returns a list of `kafka.TopicPartition`s.
func TestOffsets(tb testing.TB, message stream.Message) []kafka.TopicPartition {
	tb.Helper()

	consumer, closer := testKafkaConsumer(tb, message.ConsumerTopic, false)
	defer closer()

	tp := []kafka.TopicPartition{*stream.MessageOpqaue(&message).(opaque).toppar}
	offsets, err := consumer.Committed(tp, 2000)
	require.NoError(tb, err)

	return offsets
}

// TestConsumerConfig returns sane default options to use during testing of the
// kafkaclient consumer implementation.
func TestConsumerConfig(tb testing.TB, topicAndGroup string, options ...streamconfig.Option) []streamconfig.Option {
	var allOptions []streamconfig.Option

	if testutil.Verbose(tb) {
		verbose := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
			c.Kafka.Debug.CGRP = true
			c.Kafka.Debug.Topic = true
		})

		allOptions = append(allOptions, verbose)
	}

	opts := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.Logger = zaptest.NewLogger(tb).Named("testConsumer")
		c.Kafka = kafkaconfig.TestConsumer(tb)
		c.Kafka.GroupID = topicAndGroup
		c.Kafka.Topics = []string{topicAndGroup}
	})

	return append(append(allOptions, opts), options...)
}

// TestProducerConfig returns sane default options to use during testing of the
// kafkaclient producer implementation.
func TestProducerConfig(tb testing.TB, topic string, options ...streamconfig.Option) []streamconfig.Option {
	var allOptions []streamconfig.Option

	if testutil.Verbose(tb) {
		verbose := streamconfig.ProducerOptions(func(p *streamconfig.Producer) {
			p.Kafka.Debug.CGRP = true
			p.Kafka.Debug.Topic = true
		})

		allOptions = append(allOptions, verbose)
	}

	opts := streamconfig.ProducerOptions(func(p *streamconfig.Producer) {
		p.Logger = zaptest.NewLogger(tb).Named("testProducer")
		p.Kafka.ID = "testProducer"
		p.Kafka.SessionTimeout = 1 * time.Second
		p.Kafka.HeartbeatInterval = 150 * time.Millisecond
		p.Kafka.Brokers = []string{kafkaconfig.TestBrokerAddress}
		p.Kafka.Topic = topic
	})

	return append(append(allOptions, opts), options...)
}

func testKafkaProducer(tb testing.TB) (*kafka.Producer, func()) {
	tb.Helper()

	config := &kafka.ConfigMap{
		"client.id":            "testKafkaProducer",
		"metadata.broker.list": kafkaconfig.TestBrokerAddress,
		"go.batch.producer":    false,
		"default.topic.config": kafka.ConfigMap{"acks": 1},
	}

	if testutil.Verbose(tb) {
		_ = config.SetKey("debug", "cgrp,topic") // nolint
	}

	producer, err := kafka.NewProducer(config)
	require.NoError(tb, err)

	closer := func() {
		i := producer.Flush(testutil.MultipliedInt(tb, 1000))
		require.Zero(tb, i, "expected all messages to be flushed")

		producer.Close()
	}

	return producer, closer
}

func testKafkaConsumer(tb testing.TB, topic string, events bool) (*kafka.Consumer, func()) {
	tb.Helper()

	config, err := streamconfig.NewConsumer(TestConsumerConfig(tb, topic)...)
	require.NoError(tb, err)

	kconfig, err := config.Kafka.ConfigMap()
	require.NoError(tb, err)

	_ = kconfig.SetKey("client.id", "testKafkaConsumer")         // nolint
	_ = kconfig.SetKey("enable.partition.eof", true)             // nolint
	_ = kconfig.SetKey("go.events.channel.enable", events)       // nolint
	_ = kconfig.SetKey("go.application.rebalance.enable", false) // nolint

	consumer, err := kafka.NewConsumer(kconfig)
	require.NoError(tb, err)

	err = consumer.Subscribe(topic, nil)
	require.NoError(tb, err)

	return consumer, func() { require.NoError(tb, consumer.Close()) }
}
