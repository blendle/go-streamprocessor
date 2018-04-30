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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestIntegrationNewProducer(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)
	options := kafkaclient.TestProducerConfig(t, topic)

	producer, err := kafkaclient.NewProducer(options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.Equal(t, "*kafkaclient.producer", reflect.TypeOf(producer).String())
}

func TestIntegrationNewProducer_WithOptions(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)

	opts := streamconfig.ProducerOptions(func(p *streamconfig.Producer) {
		p.Kafka.Debug.Msg = true
		p.Kafka.SSL.KeyPassword = "test"
	})

	options := kafkaclient.TestProducerConfig(t, topic, opts)
	producer, err := kafkaclient.NewProducer(options...)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.Equal(t, false, producer.Config().(streamconfig.Producer).Kafka.Debug.Broker)
	assert.Equal(t, true, producer.Config().(streamconfig.Producer).Kafka.Debug.Msg)
	assert.Equal(t, "test", producer.Config().(streamconfig.Producer).Kafka.SSL.KeyPassword)
}

func TestIntegrationProducer_Messages(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	topic := testutil.Random(t)
	message := stream.Message{Value: []byte("Hello Universe!")}

	producer, closer := kafkaclient.TestProducer(t, topic)
	defer closer()

	select {
	case producer.Messages() <- message:
	case <-time.After(testutil.MultipliedDuration(t, 5*time.Second)):
		require.Fail(t, "Timeout while waiting for message to be delivered.")
	}

	msg := kafkaclient.TestMessageFromTopic(t, topic)
	assert.EqualValues(t, message.Value, msg.Value)
}

func TestIntegrationProducer_Messages_Ordering(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	messageCount := 5000
	topic := testutil.Random(t)

	producer, closer := kafkaclient.TestProducer(t, topic)
	defer closer()

	for i := 0; i < messageCount; i++ {
		select {
		case producer.Messages() <- stream.Message{Value: []byte(strconv.Itoa(i))}:
		case <-time.After(1 * time.Second):
			require.Fail(t, "Timeout while waiting for message to be delivered.")
		}
	}

	// We explicitly close the producer here to force flushing of any messages
	// still in the queue.
	closer()

	messages := kafkaclient.TestMessagesFromTopic(t, topic)
	assert.Len(t, messages, messageCount)

	for i, msg := range messages {
		assert.Equal(t, strconv.Itoa(i), string(msg.Value))
	}
}

func TestIntegrationProducer_Errors(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	options := streamconfig.ProducerOptions(func(c *streamconfig.Producer) {
		c.HandleErrors = true
	})

	producer, closer := kafkaclient.TestProducer(t, testutil.Random(t), options)
	defer closer()

	select {
	case err := <-producer.Errors():
		require.Error(t, err)
		assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
	case <-time.After(1 * time.Second):
		t.Fatal("expected error, got none")
	}
}

func TestIntegrationProducer_Errors_Manual(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	handler := streamconfig.ManualErrorHandling()
	producer, closer := kafkaclient.TestProducer(t, testutil.Random(t), handler)
	defer closer()

	select {
	case err := <-producer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(10 * time.Millisecond):
	}
}

func TestIntegrationProducer_NoConsumerTopic(t *testing.T) {
	t.Parallel()
	testutil.Integration(t)

	consumerTopicAndGroup := testutil.Random(t)
	kafkaclient.TestProduceMessages(t, consumerTopicAndGroup, "hello world")

	consumer, ccloser := kafkaclient.TestConsumer(t, consumerTopicAndGroup)

	producerTopic := testutil.Random(t)
	producer, pcloser := kafkaclient.TestProducer(t, producerTopic)

	msg := streamclient.TestMessageFromConsumer(t, consumer)
	msg.Value = []byte("hello universe")
	ccloser()

	select {
	case producer.Messages() <- msg:
	case <-time.After(1 * time.Second):
		require.Fail(t, "Timeout while waiting for message to be delivered.")
	}

	// We explicitly close the producer here to force flushing of any messages
	// still in the queue.
	pcloser()

	// At this point, there should be one message on the "consumer topic", and one
	// on the "producer topic". Before PR#69, the produced message would end up on
	// the original topic (see PR description for more details).
	//
	// see: https://git.io/vpRbQ
	msgs := kafkaclient.TestMessagesFromTopic(t, consumerTopicAndGroup)
	require.Len(t, msgs, 1)
	assert.Equal(t, []byte("hello world"), msgs[0].Value)

	msgs = kafkaclient.TestMessagesFromTopic(t, producerTopic)
	require.Len(t, msgs, 1)
	assert.Equal(t, []byte("hello universe"), msgs[0].Value)
}

func BenchmarkIntegrationProducer_Messages(b *testing.B) {
	testutil.Integration(b)

	topic := testutil.Random(b)
	logger, err := zap.NewDevelopment()
	require.NoError(b, err, logger)

	// We use the default (production-like) config in this benchmark, to simulate
	// real-world usage as best as possible.
	producer, err := kafkaclient.NewProducer(
		streamconfig.KafkaBroker(kafkaconfig.TestBrokerAddress),
		streamconfig.KafkaTopic(topic),
	)
	require.NoError(b, err)
	defer func() { require.NoError(b, producer.Close()) }()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg := stream.TestMessage(b, strconv.Itoa(i), fmt.Sprintf(`{"number":%d}`, i))
		msg.ProducerTopic = topic

		producer.Messages() <- msg
	}
}
