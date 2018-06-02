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
		Inmem:          inmemconfig.Consumer{},
		Kafka:          kafkaconfig.Consumer{},
		Pubsub:         pubsubconfig.Consumer{},
		Standardstream: standardstreamconfig.Consumer{},

		Global: streamconfig.Global{
			Logger:          testutil.Logger(t),
			HandleInterrupt: false,
			Name:            "",
			AllowEnvironmentBasedConfiguration: false,
		},
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ConsumerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.True(t, config.HandleInterrupt)
	assert.Equal(t, "", config.Name)
	assert.True(t, config.AllowEnvironmentBasedConfiguration)
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
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{10}[0]},
			},
		},

		"Kafka.OffsetDefault (negative)": {
			map[string]string{"CONSUMER_KAFKA_OFFSET_DEFAULT": "-10"},
			streamconfig.Consumer{
				Kafka: kafkaconfig.Consumer{OffsetDefault: &[]int64{-10}[0]},
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

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewConsumer()
	require.NoError(t, err)

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"streamconfig.Consumer", config},
		{"inmemconfig.Consumer", config.Inmem},
		{"kafkaconfig.Consumer", config.Kafka},
		{"pubsubconfig.Consumer", config.Pubsub},
		{"standardstreamconfig.Consumer", config.Standardstream},
		{"*zap.Logger", config.Logger},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, reflect.TypeOf(tt.config).String())
	}
}

func TestNewConsumer_WithOptions(t *testing.T) {
	config, err := streamconfig.NewConsumer(streamconfig.KafkaID("test"))
	require.NoError(t, err)

	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewConsumer_WithOptions_Nil(t *testing.T) {
	t.Parallel()

	_, err := streamconfig.NewConsumer(nil)
	assert.NoError(t, err)
}

func TestNewConsumer_WithOptions_NilLogger(t *testing.T) {
	t.Parallel()

	config, err := streamconfig.NewConsumer(streamconfig.Logger(nil))
	require.NoError(t, err)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}

func TestNewConsumer_WithEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer()
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewConsumer_WithEnvironmentVariables_CustomName(t *testing.T) {
	_ = os.Setenv("HELLO_CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("HELLO_CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(streamconfig.Name("hello"))
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
}

func TestNewConsumer_WithOptionsAndEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(
		streamconfig.KafkaBroker("broker2"),
		streamconfig.KafkaID("test"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker1"}, config.Kafka.Brokers)
	assert.Equal(t, "test", config.Kafka.ID)
}

func TestNewConsumer_WithOptionsWithoutEnvironmentVariables(t *testing.T) {
	_ = os.Setenv("CONSUMER_KAFKA_BROKERS", "broker1")
	defer os.Unsetenv("CONSUMER_KAFKA_BROKERS") // nolint: errcheck

	config, err := streamconfig.NewConsumer(
		streamconfig.DisableEnvironmentConfig(),
		streamconfig.KafkaBroker("broker2"),
	)
	require.NoError(t, err)

	assert.EqualValues(t, []string{"broker2"}, config.Kafka.Brokers)
}
