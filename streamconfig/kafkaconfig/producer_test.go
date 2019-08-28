package kafkaconfig_test

import (
	"errors"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var producerDefaults = map[string]interface{}{
	"queue.buffering.backpressure.threshold": 10,
}

var producerOmitempties = []string{
	"{topic}.request.required.acks",
	"message.send.max.retries",
	"statistics.interval.ms",
	"retry.backoff.ms",
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Producer{
		BatchMessageSize:       0,
		Brokers:                []string{},
		CompressionCodec:       kafkaconfig.CompressionNone,
		Debug:                  kafkaconfig.Debug{All: true},
		HeartbeatInterval:      time.Duration(0),
		ID:                     "",
		IgnoreErrors:           []kafka.ErrorCode{kafka.ErrBadMsg},
		MaxDeliveryRetries:     0,
		MaxInFlightRequests:    0,
		MaxQueueBufferDuration: time.Duration(0),
		MaxQueueSizeKBytes:     0,
		MaxQueueSizeMessages:   0,
		RequiredAcks:           kafkaconfig.AckLeader,
		RetryBackoff:           10 * time.Second,
		SecurityProtocol:       kafkaconfig.ProtocolPlaintext,
		SessionTimeout:         time.Duration(0),
		SSL:                    kafkaconfig.SSL{KeyPath: ""},
		StatisticsInterval:     time.Duration(0),
		Topic:                  "",
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ProducerDefaults
	errs := []kafka.ErrorCode{
		kafka.ErrTransport,
		kafka.ErrAllBrokersDown,
		kafka.ErrDestroy,
		kafka.ErrFail,
		kafka.ErrResolve,
		kafka.ErrLeaderNotAvailable,
		kafka.ErrNotLeaderForPartition,
		kafka.ErrRequestTimedOut,
		kafka.ErrBrokerNotAvailable,
		kafka.ErrReplicaNotAvailable,
		kafka.ErrNetworkException,
		kafka.ErrGroupCoordinatorNotAvailable,
		kafka.ErrNotCoordinatorForGroup,
		kafka.ErrNotEnoughReplicas,
		kafka.ErrNotEnoughReplicasAfterAppend,
		kafka.ErrUnknownMemberID,
	}

	assert.Equal(t, 10000, config.BatchMessageSize)
	assert.Equal(t, kafkaconfig.Debug{}, config.Debug)
	assert.Equal(t, kafkaconfig.CompressionSnappy, config.CompressionCodec)
	assert.Equal(t, 1*time.Second, config.HeartbeatInterval)
	assert.Equal(t, errs, config.IgnoreErrors)
	assert.Equal(t, 5, config.MaxDeliveryRetries)
	assert.Equal(t, 1000000, config.MaxInFlightRequests)
	assert.Equal(t, 500*time.Millisecond, config.MaxQueueBufferDuration)
	assert.Equal(t, 2097151, config.MaxQueueSizeKBytes)
	assert.Equal(t, 1000000, config.MaxQueueSizeMessages)
	assert.EqualValues(t, kafkaconfig.AckAll, config.RequiredAcks)
	assert.EqualValues(t, 15*time.Second, config.RetryBackoff)
	assert.Equal(t, kafkaconfig.ProtocolPlaintext, config.SecurityProtocol)
	assert.Equal(t, 30*time.Second, config.SessionTimeout)
	assert.Equal(t, kafkaconfig.SSL{}, config.SSL)
	assert.Equal(t, 15*time.Minute, config.StatisticsInterval)
}

func TestProducer_ConfigMap(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		cfg *kafkaconfig.Producer
		cm  *kafka.ConfigMap
	}{
		"empty": {
			&kafkaconfig.Producer{},
			&kafka.ConfigMap{},
		},

		"batchMessageSize": {
			&kafkaconfig.Producer{BatchMessageSize: 10},
			&kafka.ConfigMap{"batch.num.messages": 10},
		},

		"brokers": {
			&kafkaconfig.Producer{Brokers: []string{"1", "2", "3"}},
			&kafka.ConfigMap{"metadata.broker.list": "1,2,3"},
		},

		"brokers (empty)": {
			&kafkaconfig.Producer{Brokers: []string{}},
			&kafka.ConfigMap{},
		},

		"compressionCodec (none)": {
			&kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionNone},
			&kafka.ConfigMap{"compression.codec": "none"},
		},

		"compressionCodec (gzip)": {
			&kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionGZIP},
			&kafka.ConfigMap{"compression.codec": "gzip"},
		},

		"compressionCodec (Snappy)": {
			&kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionSnappy},
			&kafka.ConfigMap{"compression.codec": "snappy"},
		},

		"compressionCodec (lz4)": {
			&kafkaconfig.Producer{CompressionCodec: kafkaconfig.CompressionLZ4},
			&kafka.ConfigMap{"compression.codec": "lz4"},
		},

		"debug (all)": {
			&kafkaconfig.Producer{Debug: kafkaconfig.Debug{All: true, Fetch: true}},
			&kafka.ConfigMap{"debug": "all"},
		},

		"debug (multiple)": {
			&kafkaconfig.Producer{Debug: kafkaconfig.Debug{Fetch: true, CGRP: true, Broker: false}},
			&kafka.ConfigMap{"debug": "cgrp,fetch"},
		},

		"debug (empty)": {
			&kafkaconfig.Producer{Debug: kafkaconfig.Debug{}},
			&kafka.ConfigMap{},
		},

		"heartbeatInterval": {
			&kafkaconfig.Producer{HeartbeatInterval: 10 * time.Millisecond},
			&kafka.ConfigMap{"heartbeat.interval.ms": 10},
		},

		"heartbeatInterval (minutes)": {
			&kafkaconfig.Producer{HeartbeatInterval: 2 * time.Minute},
			&kafka.ConfigMap{"heartbeat.interval.ms": 120000},
		},

		"heartbeatInterval (empty)": {
			&kafkaconfig.Producer{HeartbeatInterval: 0 * time.Millisecond},
			&kafka.ConfigMap{},
		},

		"ID": {
			&kafkaconfig.Producer{ID: "hello"},
			&kafka.ConfigMap{"client.id": "hello"},
		},

		"ID (empty)": {
			&kafkaconfig.Producer{ID: ""},
			&kafka.ConfigMap{},
		},

		"maxDeliveryRetries": {
			&kafkaconfig.Producer{MaxDeliveryRetries: 10},
			&kafka.ConfigMap{"message.send.max.retries": 10},
		},

		"maxInFlightRequests": {
			&kafkaconfig.Producer{MaxInFlightRequests: 10},
			&kafka.ConfigMap{"max.in.flight.requests.per.connection": 10},
		},

		"maxDeliveryRetries (no omitempty)": {
			&kafkaconfig.Producer{MaxDeliveryRetries: 0},
			&kafka.ConfigMap{"message.send.max.retries": 0},
		},

		"maxQueueBufferDuration": {
			&kafkaconfig.Producer{MaxQueueBufferDuration: 1 * time.Minute},
			&kafka.ConfigMap{"queue.buffering.max.ms": 60000},
		},

		"maxQueueBufferDuration (empty)": {
			&kafkaconfig.Producer{MaxQueueBufferDuration: 0 * time.Second},
			&kafka.ConfigMap{},
		},

		"maxQueueSizeKBytes": {
			&kafkaconfig.Producer{MaxQueueSizeKBytes: 1024},
			&kafka.ConfigMap{"queue.buffering.max.kbytes": 1024},
		},

		"maxQueueSizeKBytes (empty)": {
			&kafkaconfig.Producer{MaxQueueSizeKBytes: 0},
			&kafka.ConfigMap{},
		},

		"maxQueueSize": {
			&kafkaconfig.Producer{MaxQueueSizeMessages: 99},
			&kafka.ConfigMap{"queue.buffering.max.messages": 99},
		},

		"maxQueueSize (empty)": {
			&kafkaconfig.Producer{MaxQueueSizeMessages: 0},
			&kafka.ConfigMap{},
		},

		"requiredAcks (none)": {
			&kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckNone},
			&kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"request.required.acks": 0}},
		},

		"requiredAcks (leader)": {
			&kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckLeader},
			&kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"request.required.acks": 1}},
		},

		"requiredAcks (all)": {
			&kafkaconfig.Producer{RequiredAcks: kafkaconfig.AckAll},
			&kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"request.required.acks": -1}},
		},

		"RetryBackoff": {
			&kafkaconfig.Producer{RetryBackoff: 1 * time.Second},
			&kafka.ConfigMap{"retry.backoff.ms": 1000},
		},

		"securityProtocol (plaintext)": {
			&kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolPlaintext},
			&kafka.ConfigMap{"security.protocol": "plaintext"},
		},

		"securityProtocol (SSL)": {
			&kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			&kafka.ConfigMap{"security.protocol": "ssl"},
		},

		"securityProtocol (SASL plaintext)": {
			&kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSASLPlaintext},
			&kafka.ConfigMap{"security.protocol": "sasl_plaintext"},
		},

		"securityProtocol (SASL SSL)": {
			&kafkaconfig.Producer{SecurityProtocol: kafkaconfig.ProtocolSASLSSL},
			&kafka.ConfigMap{"security.protocol": "sasl_ssl"},
		},

		"sessionTimeout": {
			&kafkaconfig.Producer{SessionTimeout: 2 * time.Millisecond},
			&kafka.ConfigMap{"session.timeout.ms": 2},
		},

		"sessionTimeout (seconds)": {
			&kafkaconfig.Producer{SessionTimeout: 10 * time.Second},
			&kafka.ConfigMap{"session.timeout.ms": 10000},
		},

		"sessionTimeout (invalid)": {
			// Even though this value is an invalid configuration value, we set it
			// anyway, and let the broker crash hard on boot.
			&kafkaconfig.Producer{SessionTimeout: -1 * time.Second},
			&kafka.ConfigMap{"session.timeout.ms": -1000},
		},

		"sessionTimeout (empty)": {
			&kafkaconfig.Producer{SessionTimeout: 0 * time.Second},
			&kafka.ConfigMap{},
		},

		"SSL (empty)": {
			&kafkaconfig.Producer{SSL: kafkaconfig.SSL{}},
			&kafka.ConfigMap{},
		},

		"SSL (single)": {
			&kafkaconfig.Producer{SSL: kafkaconfig.SSL{CAPath: "/tmp"}},
			&kafka.ConfigMap{"ssl.ca.location": "/tmp"},
		},

		"SSL (all)": {
			&kafkaconfig.Producer{SSL: kafkaconfig.SSL{
				CAPath:           "/tmp",
				CRLPath:          "/tmp2",
				CertPath:         "/tmp3",
				KeyPassword:      "1234",
				KeyPath:          "/tmp4",
				KeystorePassword: "5678",
				KeystorePath:     "/tmp5",
			}},
			&kafka.ConfigMap{
				"ssl.ca.location":          "/tmp",
				"ssl.crl.location":         "/tmp2",
				"ssl.certificate.location": "/tmp3",
				"ssl.key.password":         "1234",
				"ssl.key.location":         "/tmp4",
				"ssl.keystore.password":    "5678",
				"ssl.keystore.location":    "/tmp5",
			},
		},

		"statisticsInterval": {
			&kafkaconfig.Producer{StatisticsInterval: 2 * time.Second},
			&kafka.ConfigMap{"statistics.interval.ms": 2000},
		},

		"statisticsInterval (no omitempty)": {
			&kafkaconfig.Producer{StatisticsInterval: 0 * time.Millisecond},
			&kafka.ConfigMap{"statistics.interval.ms": 0},
		},

		"topic (skipped)": {
			&kafkaconfig.Producer{Topic: "a"},
			&kafka.ConfigMap{},
		},

		"multiple": {
			&kafkaconfig.Producer{
				ID:      "hello",
				Debug:   kafkaconfig.Debug{All: true},
				Brokers: []string{"1", "2"},
			},
			&kafka.ConfigMap{"metadata.broker.list": "1,2", "debug": "all", "client.id": "hello"},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			cfg, _ := tt.cfg.ConfigMap()

			// Quick fix to filter out values without the "omitempty" tag, which have
			// their value set to the `zero` value. We check if the actual config has
			// the required key set. If it has, _and_ its value is equal to the `zero`
			// value, and the expected config does not have it set, we set the value
			// for the expected config.
			for _, k := range producerOmitempties {
				if v, err := cfg.Get(k, nil); err == nil {
					if vv, _ := tt.cm.Get(k, nil); vv == nil {
						err := tt.cm.SetKey(k, v)
						require.NoError(t, err)
					}
				}
			}

			// The above table only compares the non-default values for readability.
			// In this loop, we validate that the default values are set as expected,
			// and then add them to the config, to make the rest of the validation not
			// choke on these extra configuration keys.
			for k, v := range producerDefaults {
				vv, err := cfg.Get(k, nil)
				require.NoError(t, err)
				assert.Equal(t, v, vv)

				err = tt.cm.SetKey(k, v)
				require.NoError(t, err)
			}

			assert.EqualValues(t, tt.cm, cfg)
		})
	}
}

func TestProducer_ConfigMap_Validation(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		err error
		cfg *kafkaconfig.Producer
	}{
		"empty": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Producer{},
		},

		"valid": {
			nil,
			&kafkaconfig.Producer{Brokers: []string{"1"}, Topic: "1"},
		},

		"brokers (missing)": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Producer{Topic: "1"},
		},

		"brokers (empty)": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Producer{Brokers: []string{}, Topic: "1"},
		},

		"topic (missing)": {
			errors.New("required config Kafka.Topic missing"),
			&kafkaconfig.Producer{Brokers: []string{"1"}},
		},

		"topic (empty)": {
			errors.New("required config Kafka.Topic missing"),
			&kafkaconfig.Producer{Brokers: []string{"1"}, Topic: ""},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := tt.cfg.ConfigMap()
			if tt.err == nil {
				assert.NoError(t, err)
			} else {
				assert.Equal(t, tt.err, err)
			}
		})
	}
}
