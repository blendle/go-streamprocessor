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

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Producer{
		Inmem:          inmemconfig.Producer{},
		Kafka:          kafkaconfig.Producer{},
		Pubsub:         pubsubconfig.Producer{},
		Standardstream: standardstreamconfig.Producer{},

		Global: streamconfig.Global{
			Logger:          testutil.Logger(t),
			HandleInterrupt: false,
			Name:            "",
			AllowEnvironmentBasedConfiguration: false,
		},
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ProducerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.True(t, config.HandleInterrupt)
	assert.Equal(t, "", config.Name)
	assert.True(t, config.AllowEnvironmentBasedConfiguration)
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

func TestNewProducer(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewProducer()
	require.NoError(t, err)

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"streamconfig.Producer", config},
		{"inmemconfig.Producer", config.Inmem},
		{"kafkaconfig.Producer", config.Kafka},
		{"pubsubconfig.Producer", config.Pubsub},
		{"standardstreamconfig.Producer", config.Standardstream},
		{"*zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewProducer_WithOptions(t *testing.T) {
	config, err := streamconfig.NewProducer(streamconfig.KafkaID("test"))
	require.NoError(t, err)

	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewProducer_WithOptions_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewProducer(nil)
	assert.NoError(t, err)
}

func TestNewProducer_WithOptions_NilLogger(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewProducer(streamconfig.Logger(nil))
	require.NoError(t, err)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}

func TestNewProducer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewProducer_WithEnvironmentVariables_CustomName(t *testing.T) {
	_ = os.Setenv("HELLO_WORLD_PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("HELLO_WORLD_PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(streamconfig.Name("hello_world"))
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewProducer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewProducer_WithOptionsWithoutEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewProducer(
		streamconfig.DisableEnvironmentConfig(),
		streamconfig.KafkaBroker("broker2"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker2"}, config.Kafka.Brokers)
}

func TestProducer_FromEnv(t *testing.T) {
	_ = os.Setenv("PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config := streamconfig.Producer{Kafka: kafkaconfig.Producer{}}

	config, err := config.FromEnv()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestProducer_FromEnv_CustomName(t *testing.T) {
	_ = os.Setenv("HELLO_PRODUCER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("HELLO_PRODUCER_KAFKA_BROKERS") // nolint: errcheck

	config := streamconfig.Producer{Kafka: kafkaconfig.Producer{}}
	config.Name = "hello"

	config, err := config.FromEnv()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}
