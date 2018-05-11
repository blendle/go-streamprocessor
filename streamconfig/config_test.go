package streamconfig_test

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/blendle/go-streamprocessor/streamutil/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Consumer{
		Inmem:           inmemconfig.Consumer{},
		Kafka:           kafkaconfig.Consumer{},
		Pubsub:          pubsubconfig.Consumer{},
		Standardstream:  standardstreamconfig.Consumer{},
		Logger:          testutil.Logger(t),
		HandleInterrupt: false,
		Name:            "",
		AllowEnvironmentBasedConfiguration: false,
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ConsumerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.True(t, config.HandleInterrupt)
	assert.Equal(t, "consumer", config.Name)
	assert.True(t, config.AllowEnvironmentBasedConfiguration)
}

func TestConsumerUsage(t *testing.T) {
	c := streamconfig.Consumer{Name: "test_config"}
	usage := c.Usage()

	assert.Contains(t, string(usage), "TEST_CONFIG_KAFKA_GROUP_ID")
}

func TestConsumerEnvironmentVariables(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		envs   map[string]string
		config streamconfig.Consumer
	}{
		"Kafka.Brokers": {
			map[string]string{"CONSUMER_KAFKA_BROKERS": "hello,world"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Brokers: []string{"hello", "world"}}},
		},

		"Kafka.CommitInterval": {
			map[string]string{"CONSUMER_KAFKA_COMMIT_INTERVAL": "10ms"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{CommitInterval: 10 * time.Millisecond}},
		},

		"Kafka.Debug (single)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "broker"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{Broker: true}}},
		},

		"Kafka.Debug (all)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "all"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (true)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "true"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (1)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "1"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (capitalized)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "All"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (multiple)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "broker,fetch"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Debug: kafkaconfig.Debug{Broker: true, Fetch: true}},
			},
		},

		"Kafka.Debug (invalid)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": "nope"},
			streamconfig.Consumer{},
		},

		"Kafka.Debug (empty)": {
			map[string]string{"CONSUMER_KAFKA_DEBUG": ""},
			streamconfig.Consumer{},
		},

		"Kafka.GroupID": {
			map[string]string{"CONSUMER_KAFKA_GROUP_ID": "hello"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{GroupID: "hello"}},
		},

		"Kafka.HeartbeatInterval": {
			map[string]string{"CONSUMER_KAFKA_HEARTBEAT_INTERVAL": "5s"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{HeartbeatInterval: 5 * time.Second}},
		},

		"Kafka.ID": {
			map[string]string{"CONSUMER_KAFKA_CLIENT_ID": "hi!"},
			streamconfig.Consumer{Kafka: kafkaconfig.Consumer{ID: "hi!"}},
		},

		"Kafka.MaxInFlightRequests": {
			map[string]string{"CONSUMER_KAFKA_MAX_IN_FLIGHT_REQUESTS": "10"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{MaxInFlightRequests: 10},
			},
		},

		"Kafka.OffsetDefault (positive)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_DEFAULT": "10"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: 10},
			},
		},

		"Kafka.OffsetDefault (negative)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_DEFAULT": "-10"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: -10},
			},
		},

		"Kafka.OffsetInitial (beginning)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_INITIAL": "beginning"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetBeginning},
			},
		},

		"Kafka.OffsetInitial (end)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_INITIAL": "end"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetEnd},
			},
		},

		"Kafka.OffsetInitial (capitalized)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_INITIAL": "End"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetEnd},
			},
		},

		"Kafka.SecurityProtocol (plaintext)": {
			map[string]string{"CONSUMER_KAFKA_SECURITY_PROTOCOL": "plaintext"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolPlaintext},
			},
		},

		"Kafka.SecurityProtocol (ssl)": {
			map[string]string{"CONSUMER_KAFKA_SECURITY_PROTOCOL": "SSL"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			},
		},

		"Kafka.SecurityProtocol (sasl_plaintext)": {
			map[string]string{"CONSUMER_KAFKA_SECURITY_PROTOCOL": "sasl_plaintext"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSASLPlaintext},
			},
		},

		"Kafka.SecurityProtocol (sasl_ssl)": {
			map[string]string{"CONSUMER_KAFKA_SECURITY_PROTOCOL": "sasl_ssl"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSASLSSL},
			},
		},

		"Kafka.SessionTimeout": {
			map[string]string{"CONSUMER_KAFKA_SESSION_TIMEOUT": "1h"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SessionTimeout: 1 * time.Hour},
			},
		},

		"Kafka.SSL.CAPath": {
			map[string]string{"CONSUMER_KAFKA_SSL_CA_PATH": "/tmp"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{CAPath: "/tmp"}},
			},
		},

		"Kafka.SSL.CertPath": {
			map[string]string{"CONSUMER_KAFKA_SSL_CERT_PATH": "/tmp"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{CertPath: "/tmp"}},
			},
		},

		"Kafka.SSL.CRLPath": {
			map[string]string{"CONSUMER_KAFKA_SSL_CRL_PATH": "/tmp"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{CRLPath: "/tmp"}},
			},
		},

		"Kafka.SSL.KeyPassword": {
			map[string]string{"CONSUMER_KAFKA_SSL_KEY_PASSWORD": "1234"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{KeyPassword: "1234"}},
			},
		},

		"Kafka.SSL.KeyPath": {
			map[string]string{"CONSUMER_KAFKA_SSL_KEY_PATH": "/tmp"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{KeyPath: "/tmp"}},
			},
		},

		"Kafka.SSL.KeystorePassword": {
			map[string]string{"CONSUMER_KAFKA_SSL_KEYSTORE_PASSWORD": "1234"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{KeystorePassword: "1234"}},
			},
		},

		"Kafka.SSL.KeystorePath": {
			map[string]string{"CONSUMER_KAFKA_SSL_KEYSTORE_PATH": "/tmp"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{SSL: kafkaconfig.SSL{KeystorePath: "/tmp"}},
			},
		},

		"Kafka.Topics (single)": {
			map[string]string{"CONSUMER_KAFKA_TOPICS": "hello"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{"hello"}},
			},
		},

		"Kafka.Topics (multiple)": {
			map[string]string{"CONSUMER_KAFKA_TOPICS": "hello,world"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{"hello", "world"}},
			},
		},

		// FIXME: we should probably disallow this
		"Kafka.Topics (none)": {
			map[string]string{"CONSUMER_KAFKA_TOPICS": ""},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{""}},
			},
		},

		// FIXME: we should probably disallow this
		"Kafka.Topics (empty)": {
			map[string]string{"CONSUMER_KAFKA_TOPICS": ",,"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{Topics: []string{"", "", ""}},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			for k, v := range tt.envs {
				_ = os.Setenv(k, v)
				defer func(k string) { require.NoError(t, os.Unsetenv(k)) }(k)
			}

			tt.config.Name = "consumer"
			tt.config.AllowEnvironmentBasedConfiguration = true
			options := streamconfig.ConsumerOptions(func(c *streamconfig.Consumer) {
				c.Name = "consumer"
				c.AllowEnvironmentBasedConfiguration = true
			})

			assert.EqualValues(t, tt.config, streamconfig.TestNewConsumer(t, false, options))
		})
	}
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Producer{
		Inmem:           inmemconfig.Producer{},
		Kafka:           kafkaconfig.Producer{},
		Pubsub:          pubsubconfig.Producer{},
		Standardstream:  standardstreamconfig.Producer{},
		Logger:          testutil.Logger(t),
		HandleInterrupt: false,
		Name:            "",
		AllowEnvironmentBasedConfiguration: false,
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ProducerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.True(t, config.HandleInterrupt)
	assert.Equal(t, "producer", config.Name)
	assert.True(t, config.AllowEnvironmentBasedConfiguration)
}

func TestProducerUsage(t *testing.T) {
	p := streamconfig.Producer{Name: "test_config"}
	usage := p.Usage()

	assert.Contains(t, string(usage), "TEST_CONFIG_KAFKA_MAX_DELIVERY_RETRIES")
}

func TestProducerEnvironmentVariables(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		envs   map[string]string
		config streamconfig.Producer
	}{
		"Kafka.BatchMessageSize": {
			map[string]string{"PRODUCER_KAFKA_BATCH_MESSAGE_SIZE": "10000"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{BatchMessageSize: 10000},
			},
		},

		"Kafka.CompressionCodec (none)": {
			map[string]string{"PRODUCER_KAFKA_COMPRESSION_CODEC": "none"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionNone},
			},
		},

		"Kafka.CompressionCodec (gzip)": {
			map[string]string{"PRODUCER_KAFKA_COMPRESSION_CODEC": "gzip"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionGZIP},
			},
		},

		"Kafka.CompressionCodec (snappy)": {
			map[string]string{"PRODUCER_KAFKA_COMPRESSION_CODEC": "snappy"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionSnappy},
			},
		},

		"Kafka.CompressionCodec (lz4)": {
			map[string]string{"PRODUCER_KAFKA_COMPRESSION_CODEC": "lz4"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionLZ4},
			},
		},

		"Kafka.CompressionCodec (capitalized)": {
			map[string]string{"PRODUCER_KAFKA_COMPRESSION_CODEC": "LZ4"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionLZ4},
			},
		},

		"Kafka.Brokers": {
			map[string]string{"PRODUCER_KAFKA_BROKERS": "hello,world"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Brokers: []string{"hello", "world"}}},
		},

		"Kafka.Debug (single)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "broker"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{Broker: true}}},
		},

		"Kafka.Debug (all)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "all"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (true)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "true"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (1)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "1"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (capitalized)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "All"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true}}},
		},

		"Kafka.Debug (multiple)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "broker,fetch"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Debug: kafkaconfig.Debug{Broker: true, Fetch: true}},
			},
		},

		"Kafka.Debug (invalid)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": "nope"},
			streamconfig.Producer{},
		},

		"Kafka.Debug (empty)": {
			map[string]string{"PRODUCER_KAFKA_DEBUG": ""},
			streamconfig.Producer{},
		},

		"Kafka.HeartbeatInterval": {
			map[string]string{"PRODUCER_KAFKA_HEARTBEAT_INTERVAL": "5s"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{HeartbeatInterval: 5 * time.Second}},
		},

		"Kafka.ID": {
			map[string]string{"PRODUCER_KAFKA_CLIENT_ID": "hi!"},
			streamconfig.Producer{Kafka: kafkaconfig.Producer{ID: "hi!"}},
		},

		"Kafka.MaxDeliveryRetries": {
			map[string]string{"PRODUCER_KAFKA_MAX_DELIVERY_RETRIES": "1"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxDeliveryRetries: 1},
			},
		},

		"Kafka.MaxDeliveryRetries (zero)": {
			map[string]string{"PRODUCER_KAFKA_MAX_DELIVERY_RETRIES": "0"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxDeliveryRetries: 0},
			},
		},

		"Kafka.MaxQueueBufferDuration": {
			map[string]string{"PRODUCER_KAFKA_MAX_QUEUE_BUFFER_DURATION": "1m"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueBufferDuration: 1 * time.Minute},
			},
		},

		"Kafka.MaxQueueSizeKBytes": {
			map[string]string{"PRODUCER_KAFKA_MAX_QUEUE_SIZE_KBYTES": "100"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueSizeKBytes: 100},
			},
		},

		"Kafka.MaxQueueSizeMessages": {
			map[string]string{"PRODUCER_KAFKA_MAX_QUEUE_SIZE_MESSAGES": "1"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueSizeMessages: 1},
			},
		},

		"Kafka.MaxQueueSizeMessages (zero)": {
			map[string]string{"PRODUCER_KAFKA_MAX_QUEUE_SIZE_MESSAGES": "0"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{MaxQueueSizeMessages: 0},
			},
		},

		"Kafka.RequiredAcks (leader)": {
			map[string]string{"PRODUCER_KAFKA_REQUIRED_ACKS": "1"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckLeader},
			},
		},

		"Kafka.RequiredAcks (all)": {
			map[string]string{"PRODUCER_KAFKA_REQUIRED_ACKS": "-1"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckAll},
			},
		},

		"Kafka.RequiredAcks (none)": {
			map[string]string{"PRODUCER_KAFKA_REQUIRED_ACKS": "0"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckNone},
			},
		},

		"Kafka.SecurityProtocol (plaintext)": {
			map[string]string{"PRODUCER_KAFKA_SECURITY_PROTOCOL": "plaintext"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolPlaintext},
			},
		},

		"Kafka.SecurityProtocol (ssl)": {
			map[string]string{"PRODUCER_KAFKA_SECURITY_PROTOCOL": "SSL"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			},
		},

		"Kafka.SecurityProtocol (sasl_plaintext)": {
			map[string]string{"PRODUCER_KAFKA_SECURITY_PROTOCOL": "sasl_plaintext"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSASLPlaintext},
			},
		},

		"Kafka.SecurityProtocol (sasl_ssl)": {
			map[string]string{"PRODUCER_KAFKA_SECURITY_PROTOCOL": "sasl_ssl"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSASLSSL},
			},
		},

		"Kafka.SessionTimeout": {
			map[string]string{"PRODUCER_KAFKA_SESSION_TIMEOUT": "1h"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SessionTimeout: 1 * time.Hour},
			},
		},

		"Kafka.SSL.CAPath": {
			map[string]string{"PRODUCER_KAFKA_SSL_CA_PATH": "/tmp"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{CAPath: "/tmp"}},
			},
		},

		"Kafka.SSL.CertPath": {
			map[string]string{"PRODUCER_KAFKA_SSL_CERT_PATH": "/tmp"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{CertPath: "/tmp"}},
			},
		},

		"Kafka.SSL.CRLPath": {
			map[string]string{"PRODUCER_KAFKA_SSL_CRL_PATH": "/tmp"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{CRLPath: "/tmp"}},
			},
		},

		"Kafka.SSL.KeyPassword": {
			map[string]string{"PRODUCER_KAFKA_SSL_KEY_PASSWORD": "1234"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{KeyPassword: "1234"}},
			},
		},

		"Kafka.SSL.KeyPath": {
			map[string]string{"PRODUCER_KAFKA_SSL_KEY_PATH": "/tmp"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{KeyPath: "/tmp"}},
			},
		},

		"Kafka.SSL.KeystorePassword": {
			map[string]string{"PRODUCER_KAFKA_SSL_KEYSTORE_PASSWORD": "1234"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{KeystorePassword: "1234"}},
			},
		},

		"Kafka.SSL.KeystorePath": {
			map[string]string{"PRODUCER_KAFKA_SSL_KEYSTORE_PATH": "/tmp"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{SSL: kafkaconfig.SSL{KeystorePath: "/tmp"}},
			},
		},

		"Kafka.Topic": {
			map[string]string{"PRODUCER_KAFKA_TOPIC": "hello"},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Topic: "hello"},
			},
		},

		"Kafka.Topics (empty)": {
			map[string]string{"PRODUCER_KAFKA_TOPIC": ""},
			streamconfig.Producer{
				Kafka: kafkaconfig.Producer{Topic: ""},
			},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			for k, v := range tt.envs {
				_ = os.Setenv(k, v)
				defer func(k string) { require.NoError(t, os.Unsetenv(k)) }(k)
			}

			tt.config.Name = "producer"
			tt.config.AllowEnvironmentBasedConfiguration = true
			options := streamconfig.ProducerOptions(func(c *streamconfig.Producer) {
				c.Name = "producer"
				c.AllowEnvironmentBasedConfiguration = true
			})

			assert.EqualValues(t, tt.config, streamconfig.TestNewProducer(t, false, options))
		})
	}
}
