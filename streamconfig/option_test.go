package streamconfig_test

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestOptions(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		ins      []streamconfig.Option
		consumer streamconfig.Consumer
		producer streamconfig.Producer
	}{
		"DisableEnvironmentConfig": {
			[]streamconfig.Option{streamconfig.DisableEnvironmentConfig()},
			streamconfig.Consumer{
				AllowEnvironmentBasedConfiguration: false,
			},
			streamconfig.Producer{
				AllowEnvironmentBasedConfiguration: false,
			},
		},

		"ManualErrorHandling": {
			[]streamconfig.Option{streamconfig.ManualErrorHandling()},
			streamconfig.Consumer{
				HandleErrors: false,
			},
			streamconfig.Producer{
				HandleErrors: false,
			},
		},

		"ManualInterruptHandling": {
			[]streamconfig.Option{streamconfig.ManualInterruptHandling()},
			streamconfig.Consumer{
				HandleInterrupt: false,
			},
			streamconfig.Producer{
				HandleInterrupt: false,
			},
		},

		"Logger": {
			[]streamconfig.Option{streamconfig.Logger(zap.NewNop())},
			streamconfig.Consumer{
				Logger: zap.NewNop(),
			},
			streamconfig.Producer{
				Logger: zap.NewNop(),
			},
		},

		"Name": {
			[]streamconfig.Option{streamconfig.Name("test1")},
			streamconfig.Consumer{
				Name: "test1",
			},
			streamconfig.Producer{
				Name: "test1",
			},
		},

		"InmemStore": {
			[]streamconfig.Option{streamconfig.InmemStore(inmemstore.New())},
			streamconfig.Consumer{
				Inmem: inmemconfig.Consumer{Store: inmemstore.New()},
			},
			streamconfig.Producer{
				Inmem: inmemconfig.Producer{Store: inmemstore.New()},
			},
		},

		"KafkaBroker": {
			[]streamconfig.Option{streamconfig.KafkaBroker("test1")},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Brokers: []string{"test1"}},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Brokers: []string{"test1"}},
			},
		},

		"KafkaBroker (multiple)": {
			[]streamconfig.Option{
				streamconfig.KafkaBroker("test1"),
				streamconfig.KafkaBroker("test2"),
			},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Brokers: []string{"test1", "test2"}},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Brokers: []string{"test1", "test2"}},
			},
		},

		"KafkaCommitInterval": {
			[]streamconfig.Option{streamconfig.KafkaCommitInterval(1 * time.Second)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{CommitInterval: 1 * time.Second},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaCompressionCodec": {
			[]streamconfig.Option{streamconfig.KafkaCompressionCodec(kafkaconfig.CompressionGZIP)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionGZIP},
			},
		},

		"KafkaDebug": {
			[]streamconfig.Option{streamconfig.KafkaDebug()},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true}},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true}},
			},
		},

		"KafkaGroupID": {
			[]streamconfig.Option{streamconfig.KafkaGroupID("test1")},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{GroupID: "test1"},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaHeartbeatInterval": {
			[]streamconfig.Option{streamconfig.KafkaHeartbeatInterval(1 * time.Second)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{HeartbeatInterval: 1 * time.Second},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{HeartbeatInterval: 1 * time.Second},
			},
		},

		"KafkaID": {
			[]streamconfig.Option{streamconfig.KafkaID("test1")},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{ID: "test1"},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{ID: "test1"},
			},
		},

		"KafkaInitialOffset": {
			[]streamconfig.Option{streamconfig.KafkaInitialOffset(kafkaconfig.OffsetEnd)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{InitialOffset: kafkaconfig.OffsetEnd},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaMaxDeliveryRetries": {
			[]streamconfig.Option{streamconfig.KafkaMaxDeliveryRetries(10)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxDeliveryRetries: 10},
			},
		},

		"KafkaMaxQueueBufferDuration": {
			[]streamconfig.Option{streamconfig.KafkaMaxQueueBufferDuration(1 * time.Second)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueBufferDuration: 1 * time.Second},
			},
		},

		"KafkaMaxQueueSizeKBytes": {
			[]streamconfig.Option{streamconfig.KafkaMaxQueueSizeKBytes(1024)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueSizeKBytes: 1024},
			},
		},

		"KafkaMaxQueueSizeMessages": {
			[]streamconfig.Option{streamconfig.KafkaMaxQueueSizeMessages(5000)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueSizeMessages: 5000},
			},
		},

		"KafkaRequireNoAck": {
			[]streamconfig.Option{streamconfig.KafkaRequireNoAck()},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckNone},
			},
		},

		"KafkaRequireLeaderAck": {
			[]streamconfig.Option{streamconfig.KafkaRequireLeaderAck()},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckLeader},
			},
		},

		"KafkaRequireAllAck": {
			[]streamconfig.Option{streamconfig.KafkaRequireAllAck()},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckAll},
			},
		},

		"KafkaSecurityProtocol": {
			[]streamconfig.Option{streamconfig.KafkaSecurityProtocol(kafkaconfig.ProtocolSSL)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			},
		},

		"KafkaSessionTimeout": {
			[]streamconfig.Option{streamconfig.KafkaSessionTimeout(1 * time.Second)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SessionTimeout: 1 * time.Second},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SessionTimeout: 1 * time.Second},
			},
		},

		"KafkaSSL": {
			[]streamconfig.Option{streamconfig.KafkaSSL("a", "b", "c", "d", "e", "f", "g")},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{
					SSL: kafkaconfig.SSL{
						CAPath:           "a",
						CertPath:         "b",
						CRLPath:          "c",
						KeyPassword:      "d",
						KeyPath:          "e",
						KeystorePassword: "f",
						KeystorePath:     "g",
					},
				},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{
					SSL: kafkaconfig.SSL{
						CAPath:           "a",
						CertPath:         "b",
						CRLPath:          "c",
						KeyPassword:      "d",
						KeyPath:          "e",
						KeystorePassword: "f",
						KeystorePath:     "g",
					},
				},
			},
		},

		"KafkaTopic": {
			[]streamconfig.Option{streamconfig.KafkaTopic("test1")},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{"test1"}},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Topic: "test1"},
			},
		},

		"KafkaTopic (multiple)": {
			[]streamconfig.Option{
				streamconfig.KafkaTopic("test1"),
				streamconfig.KafkaTopic("test2"),
			},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{"test1", "test2"}},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Topic: "test2"},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := streamconfig.Consumer{}
			p := streamconfig.Producer{}

			assert.EqualValues(t, tt.consumer, c.WithOptions(tt.ins...))
			assert.EqualValues(t, tt.producer, p.WithOptions(tt.ins...))
		})
	}
}

func TestConsumerOptions(t *testing.T) {
	opts := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.Name = "hello world"
	})

	c1 := streamconfig.Consumer{}
	c2, _ := streamconfig.NewConsumer()
	c2.Name = "hello world"

	p := streamconfig.Producer{}

	assert.EqualValues(t, c2, c1.WithOptions(opts))
	assert.EqualValues(t, streamconfig.Producer{}, p.WithOptions(opts))
}

func TestProducerOptions(t *testing.T) {
	opts := streamconfig.ProducerOptions(func(p *streamconfig.Producer) {
		p.Name = "hello world"
	})

	p1 := streamconfig.Producer{}
	p2, _ := streamconfig.NewProducer()
	p2.Name = "hello world"

	c := streamconfig.Consumer{}

	assert.EqualValues(t, p2, p1.WithOptions(opts))
	assert.EqualValues(t, streamconfig.Consumer{}, c.WithOptions(opts))
}
