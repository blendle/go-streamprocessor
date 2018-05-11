package streamconfig_test

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/blendle/go-streamprocessor/streamstore/inmemstore"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

		"InmemListen": {
			[]streamconfig.Option{streamconfig.InmemListen()},
			streamconfig.Consumer{
				Inmem: inmemconfig.Consumer{ConsumeOnce: false},
			},
			streamconfig.Producer{
				Inmem: inmemconfig.Producer{},
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

		"KafkaMaxDeliveryRetries": {
			[]streamconfig.Option{streamconfig.KafkaMaxDeliveryRetries(10)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxDeliveryRetries: 10},
			},
		},

		"KafkaMaxInFlightRequests": {
			[]streamconfig.Option{streamconfig.KafkaMaxInFlightRequests(10)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{MaxInFlightRequests: 10},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxInFlightRequests: 10},
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

		"KafkaOffsetHead": {
			[]streamconfig.Option{streamconfig.KafkaOffsetHead(10)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{10}[0]},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaOffsetHead (MaxUint32)": {
			[]streamconfig.Option{streamconfig.KafkaOffsetHead(math.MaxUint32)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{int64(math.MaxUint32)}[0]},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaOffsetInitial": {
			[]streamconfig.Option{streamconfig.KafkaOffsetInitial(kafkaconfig.OffsetEnd)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetEnd},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaOffsetTail": {
			[]streamconfig.Option{streamconfig.KafkaOffsetTail(10)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{-10}[0]},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaOffsetTail (MaxUint32)": {
			[]streamconfig.Option{streamconfig.KafkaOffsetTail(math.MaxUint32)},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{-int64(math.MaxUint32)}[0]},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{},
			},
		},

		"KafkaOrderedDelivery": {
			[]streamconfig.Option{streamconfig.KafkaOrderedDelivery()},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{},
			},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxInFlightRequests: 1},
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

		"StandardstreamWriter": {
			[]streamconfig.Option{
				streamconfig.StandardstreamWriter(standardstreamclient.TestBuffer(t, "")),
			},
			streamconfig.Consumer{
				Standardstream: standardstreamconfig.Consumer{},
			},
			streamconfig.Producer{
				Standardstream: standardstreamconfig.Producer{
					Writer: standardstreamclient.TestBuffer(t, ""),
				},
			},
		},

		"StandardstreamReader": {
			[]streamconfig.Option{
				streamconfig.StandardstreamReader(standardstreamclient.TestBuffer(t, "")),
			},
			streamconfig.Consumer{
				Standardstream: standardstreamconfig.Consumer{
					Reader: standardstreamclient.TestBuffer(t, ""),
				},
			},
			streamconfig.Producer{
				Standardstream: standardstreamconfig.Producer{},
			},
		}}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			c := streamconfig.Consumer{}
			p := streamconfig.Producer{}

			assert.EqualValues(t, tt.consumer, c.WithOptions(tt.ins...))
			assert.EqualValues(t, tt.producer, p.WithOptions(tt.ins...))
		})
	}
}

func TestOptions_KafkaGroupIDRandom(t *testing.T) {
	c := streamconfig.Consumer{}
	p := streamconfig.Producer{}

	c = c.WithOptions(streamconfig.KafkaGroupIDRandom())
	p = p.WithOptions(streamconfig.KafkaGroupIDRandom())

	id := strings.Replace(c.Kafka.GroupID, "processor-", "", -1)
	uid, err := uuid.FromString(id)
	require.NoError(t, err)

	assert.Equal(t, uint8(4), uid.Version())
	assert.EqualValues(t, streamconfig.Producer{}, p)
}

func TestConsumerOptions(t *testing.T) {
	opts := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.Name = "hello world"
	})

	c1 := streamconfig.Consumer{}
	c2 := streamconfig.Consumer{}
	c2.Name = "hello world"

	p := streamconfig.Producer{}

	assert.EqualValues(t, c2, c1.WithOptions(opts))
	assert.EqualValues(t, streamconfig.Producer{}, p.WithOptions(opts))
}

func TestConsumerOptions_Multiple(t *testing.T) {
	opts1 := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		// Due to an earlier bug, the default values would override the configured
		// values when the ConsumerOptions() function was called twice. This sets a
		// value to the non-default value, and it expects this value to remain in
		// the final struct.
		c.Kafka.Debug.Msg = true
	})

	opts2 := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
		c.Name = "test"
	})

	consumer := streamconfig.Consumer{}
	consumer = consumer.WithOptions(opts1, opts2)

	assert.True(t, consumer.Kafka.Debug.Msg)
	assert.Equal(t, "test", consumer.Name)
}

func TestProducerOptions(t *testing.T) {
	opts := streamconfig.ProducerOptions(func(p *streamconfig.Producer) {
		p.Name = "hello world"
	})

	p1 := streamconfig.Producer{}
	p2 := streamconfig.Producer{}
	p2.Name = "hello world"

	c := streamconfig.Consumer{}

	assert.EqualValues(t, p2, p1.WithOptions(opts))
	assert.EqualValues(t, streamconfig.Consumer{}, c.WithOptions(opts))
}

func TestProducerOptions_Multiple(t *testing.T) {
	opts1 := streamconfig.ProducerOptions(func(c *streamconfig.Producer) {
		// Due to an earlier bug, the default values would override the configured
		// values when the ProducerOptions() function was called twice. This sets a
		// value to the non-default value, and it expects this value to remain in
		// the final struct.
		c.Kafka.Debug.Msg = true
	})

	opts2 := streamconfig.ProducerOptions(func(c *streamconfig.Producer) {
		c.Name = "test"
	})

	producer := streamconfig.Producer{}
	producer = producer.WithOptions(opts1, opts2)

	assert.True(t, producer.Kafka.Debug.Msg)
	assert.Equal(t, "test", producer.Name)
}
