package kafkaclient_test

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegrationNewConsumer(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)
	options := kafkaclient.TestConsumerConfig(t, topicAndGroup)

	consumer, err := kafkaclient.NewConsumer(options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	assert.Equal(t, "*kafkaclient.consumer", reflect.TypeOf(consumer).String())
}

func TestIntegrationNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)

	opts := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.Kafka.Debug.Msg = true
		c.Kafka.SSL.KeyPassword = "test"
	})

	options := kafkaclient.TestConsumerConfig(t, topicAndGroup, opts)

	consumer, err := kafkaclient.NewConsumer(options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, consumer.Close()) }()

	assert.Equal(t, false, consumer.Config().(streamconfig.Consumer).Kafka.Debug.Broker)
	assert.Equal(t, true, consumer.Config().(streamconfig.Consumer).Kafka.Debug.Msg)
	assert.Equal(t, "test", consumer.Config().(streamconfig.Consumer).Kafka.SSL.KeyPassword)
}

func TestIntegrationConsumer_Close(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)
	options := kafkaclient.TestConsumerConfig(t, topicAndGroup)

	consumer, err := kafkaclient.NewConsumer(options...)
	require.NoError(t, err)

	ch := make(chan error)
	go func() {
		ch <- consumer.Close() // Close is working as expected, and the consumer is terminated.
		ch <- consumer.Close() // Close should return nil immediately, due to `sync.Once`.
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-ch:
			assert.NoError(t, err)
		case <-time.After(testutil.MultipliedDuration(t, 1*time.Second)):
			t.Fatal("timeout while waiting for close to finish")
		}
	}
}

func TestIntegrationConsumer_Close_WithoutInterrupt(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)
	options := kafkaclient.TestConsumerConfig(t, topic, streamconfig.ManualInterruptHandling())

	consumer, err := kafkaclient.NewConsumer(options...)
	require.NoError(t, err)

	ch := make(chan error)
	go func() {
		ch <- consumer.Close() // Close is working as expected, and the consumer is terminated.
		ch <- consumer.Close() // Close should return nil immediately, due to `sync.Once`.
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-ch:
			assert.NoError(t, err)
		case <-time.After(testutil.MultipliedDuration(t, 1*time.Second)):
			t.Fatal("timeout while waiting for close to finish")
		}
	}
}

func TestIntegrationConsumer_Messages(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicAndGroup := testutil.Random(t)
	message := kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topicAndGroup,
			Partition: kafka.PartitionAny, // nolint: gotypex
		},
		Value: []byte("hello world"),
	}

	kafkaclient.TestProduceMessages(t, topicAndGroup, message)

	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup)
	defer closer()

	select {
	case actual := <-consumer.Messages():
		assert.EqualValues(t, message.Value, actual.Value)
	case <-time.After(testutil.MultipliedDuration(t, 5*time.Second)):
		require.Fail(t, "Timeout while waiting for message to be returned.")
	}
}

func TestIntegrationConsumer_Messages_Ordering(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	messageCount := 5000
	topicAndGroup := testutil.Random(t)

	messages := []interface{}{}
	for i := 0; i < messageCount; i++ {
		message := stream.TestMessage(t, strconv.Itoa(i), "hello world"+strconv.Itoa(i))
		messages = append(messages, message)
	}

	kafkaclient.TestProduceMessages(t, topicAndGroup, messages...)

	consumer, closer := kafkaclient.TestConsumer(t, topicAndGroup)
	defer closer()

	timeout := time.NewTimer(5000 * time.Millisecond)
	i := 0
	run := true
	for run {
		select {
		case msg := <-consumer.Messages():
			timeout.Reset(5000 * time.Millisecond)

			require.Equal(t, "hello world"+strconv.Itoa(i), string(msg.Value))
			require.Equal(t, strconv.Itoa(i), string(msg.Key))

			err := consumer.Ack(msg)
			require.NoError(t, err)
			i++

			if i == messageCount {
				run = false
			}
		case <-timeout.C:
			require.Fail(t, "Timeout while waiting for message to be returned.")
		}
	}

	assert.Equal(t, messageCount, i)
}

func TestIntegrationConsumer_Errors(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	options := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.HandleErrors = true
	})

	consumer, closer := kafkaclient.TestConsumer(t, testutil.Random(t), options)
	defer closer()

	// Give the consumer some time to start.
	//
	// See: https://git.io/vpt2L
	// See: https://git.io/vpqKr
	time.Sleep(100 * time.Millisecond)

	select {
	case err := <-consumer.Errors():
		require.Error(t, err)
		assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
	case <-time.After(testutil.MultipliedDuration(t, 1*time.Second)):
		t.Fatal("expected error, got none")
	}
}

func TestIntegrationConsumer_Errors_Manual(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	handler := streamconfig.ManualErrorHandling()
	consumer, closer := kafkaclient.TestConsumer(t, testutil.Random(t), handler)
	defer closer()

	// Give the consumer some time to properly start, before shutting it down.
	//
	// See: https://git.io/vpt2L
	// See: https://git.io/vpqKr
	time.Sleep(100 * time.Millisecond)

	select {
	case err := <-consumer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(1000 * time.Millisecond):
	}
}

func TestIntegrationConsumer_Ack(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicOrGroup := testutil.Random(t)

	kafkaclient.TestProduceMessages(t, topicOrGroup, "hello world", "hello universe!")

	consumer, closer := kafkaclient.TestConsumer(t, topicOrGroup)

	message := streamclient.TestMessageFromConsumer(t, consumer)

	// Without ack'ing the message, the offset for the consumer group is still set
	// to -1001 (which is a special int, signaling "no offset available yet").
	//
	// FIXME: this check is disabled for now. Reason being the fact that when we
	//        fetch the offsets from this channel, we do so by connecting another
	//        consumer to the same toppar (since we want to know the offset of
	//        that specific toppar). This triggers a rebalance, which in turn
	//        triggers the `handleRevokedPartitions` event. But, since we've
	//        produced two messages on this topic and group (a bit above,
	//        `TestProduceMessages`), but we only consumed one message above this
	//        line, we already received the second message, but are waiting for it
	//        to be delivered to a receiver on the other end of the channel. Since
	//        there is none, this message is blocked, and thus no other events are
	//        being handled, amongst them the `handleRevokedPartitions` event.
	//        This causes the repartition event to hang, and thus the closing of
	//        this second consumer to hang.
	//
	//        You can read more about this here: https://git.io/vAixJ
	//
	//        A proposed solution is documented (but not yet implemented) here:
	//        * https://git.io/vAixY
	//        * https://git.io/vAixO
	//
	// offsets := kafkaclient.TestOffsets(t, message)
	// assert.Equal(t, int64(-1001), int64(offsets[0].Offset))

	err := consumer.Ack(message)
	require.NoError(t, err)

	// Ack does not actually send a signal to Kafka synchronously, so we have to
	// force this signal to be delivered first, before this ack has any effect. By
	// shutting down the consumer, we force this behavior. In a non-test
	// environment, this is handled asynchronously in the background.
	closer()

	// After ack'ing the message, the offset for the consumer group is increased
	// by one.
	offsets := kafkaclient.TestOffsets(t, message)
	assert.Equal(t, int64(1), int64(offsets[0].Offset))

	consumer, closer = kafkaclient.TestConsumer(t, topicOrGroup)
	message = streamclient.TestMessageFromConsumer(t, consumer)

	// Ack'ing the second message results in another increase in offset.
	err = consumer.Ack(message)
	require.NoError(t, err)
	closer()

	offsets = kafkaclient.TestOffsets(t, message)
	assert.Equal(t, int64(2), int64(offsets[0].Offset))
}

func TestIntegrationConsumer_Ack_WithClosedConsumer(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicOrGroup := testutil.Random(t)

	kafkaclient.TestProduceMessages(t, topicOrGroup, "hello world")

	consumer, closer := kafkaclient.TestConsumer(t, topicOrGroup)
	message := streamclient.TestMessageFromConsumer(t, consumer)
	closer()

	err := consumer.Ack(message)
	assert.Error(t, err)
}

func TestIntegrationConsumer_Nack(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	consumer, closer := kafkaclient.TestConsumer(t, testutil.Random(t))
	defer closer()

	assert.Nil(t, consumer.Nack(stream.Message{}))
}

func TestIntegrationConsumer_OffsetTail(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicOrGroup := testutil.Random(t)

	kafkaclient.TestProduceMessages(t, topicOrGroup, "hello", "hi", "good", "bye")

	consumer, closer := kafkaclient.TestConsumer(t, topicOrGroup, streamconfig.KafkaOffsetTail(3))
	message := streamclient.TestMessageFromConsumer(t, consumer)

	err := consumer.Ack(message)
	require.NoError(t, err)
	closer()

	// The consumer is configured to get the third message from the end.
	assert.Equal(t, "hi", string(message.Value))

	// After ack'ing the message, the offset for the consumer group is increased
	// by one.
	offsets := kafkaclient.TestOffsets(t, message)
	assert.Equal(t, int64(2), int64(offsets[0].Offset))

	// At this point, we've committed the offset for our consumer group, so even
	// though we've configured to get the third message from the end, the consumer
	// will instead continue consuming from the last known partition offset.
	consumer, closer = kafkaclient.TestConsumer(t, topicOrGroup, streamconfig.KafkaOffsetTail(3))
	message = streamclient.TestMessageFromConsumer(t, consumer)
	closer()

	assert.Equal(t, "good", string(message.Value))
}

func TestIntegrationConsumer_OffsetHead(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topicOrGroup := testutil.Random(t)

	kafkaclient.TestProduceMessages(t, topicOrGroup, "hello", "hi", "good", "bye")

	consumer, closer := kafkaclient.TestConsumer(t, topicOrGroup, streamconfig.KafkaOffsetHead(1))
	message := streamclient.TestMessageFromConsumer(t, consumer)

	err := consumer.Ack(message)
	require.NoError(t, err)
	closer()

	// The consumer is configured to get the 2nd message from the start (0-based index).
	assert.Equal(t, "hi", string(message.Value))

	// After ack'ing the message, the offset for the consumer group is increased
	// by one.
	offsets := kafkaclient.TestOffsets(t, message)
	assert.Equal(t, int64(2), int64(offsets[0].Offset))

	// At this point, we've committed the offset for our consumer group, so even
	// though we've configured a hard starting offset for the 2nd message, the
	// consumer will instead continue consuming from the last known partition
	// offset.
	consumer, closer = kafkaclient.TestConsumer(t, topicOrGroup, streamconfig.KafkaOffsetHead(1))
	message = streamclient.TestMessageFromConsumer(t, consumer)
	closer()

	assert.Equal(t, "good", string(message.Value))
}

func BenchmarkIntegrationConsumer_Messages(b *testing.B) {
	testutil.Integration(b)

	topicAndGroup := testutil.Random(b)
	line := `{"number":%d}` + "\n"

	config := &kafka.ConfigMap{
		"metadata.broker.list":         kafkaconfig.TestBrokerAddress,
		"go.batch.producer":            true,
		"go.delivery.reports":          false,
		"queue.buffering.max.messages": b.N,
		"default.topic.config":         kafka.ConfigMap{"acks": 1},
	}

	producer, err := kafka.NewProducer(config)
	require.NoError(b, err)

	msg := stream.TestMessage(b, "", "")
	h := []kafka.Header{}
	for k, v := range msg.Tags {
		h = append(h, kafka.Header{Key: k, Value: v})
	}

	tp := kafka.TopicPartition{
		Topic:     &topicAndGroup,
		Partition: kafka.PartitionAny, // nolint: gotypex
	}

	for i := 1; i <= b.N; i++ {
		m := &kafka.Message{
			Key:            msg.Key,
			Value:          []byte(fmt.Sprintf(line, i)),
			Timestamp:      msg.Timestamp,
			Headers:        h,
			TopicPartition: tp,
		}

		require.NoError(b, producer.Produce(m, nil))
	}

	require.Zero(b, producer.Flush(10000), "messages remain in queue after Flush()")
	producer.Close()

	// We use the default (production-like) config in this benchmark, to simulate
	// real-world usage as best as possible.
	consumer, err := kafkaclient.NewConsumer(
		streamconfig.KafkaBroker(kafkaconfig.TestBrokerAddress),
		streamconfig.KafkaGroupID(topicAndGroup),
		streamconfig.KafkaTopic(topicAndGroup),
	)
	require.NoError(b, err)
	defer func() { require.NoError(b, consumer.Close()) }()

	i := 0

	b.ResetTimer()
	for {
		select {
		case <-consumer.Messages():
			i++

			if i == b.N {
				return
			}
		case <-time.After(5 * time.Second):
			assert.Fail(b, "timeout waiting for messages to be delivered", "got %d of %d messages", i, b.N)
		}
	}
}
