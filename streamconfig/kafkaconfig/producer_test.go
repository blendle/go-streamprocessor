package kafkaconfig_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var producerDefaults = map[string]interface{}{
	"queue.buffering.backpressure.threshold": 100,
	"compression.codec":                      "snappy",
	"batch.num.messages":                     100000,
}

var producerOmitempties = []string{
	"{topic}.request.required.acks",
	"message.send.max.retries",
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Producer{
		Brokers:                []string{},
		Debug:                  kafkaconfig.Debug{All: true},
		HeartbeatInterval:      time.Duration(0),
		ID:                     "",
		Logger:                 *zap.NewNop(),
		MaxDeliveryRetries:     0,
		MaxQueueBufferDuration: time.Duration(0),
		MaxQueueSizeKBytes:     0,
		MaxQueueSize:           0,
		RequiredAcks:           kafkaconfig.AckAll,
		SecurityProtocol:       kafkaconfig.ProtocolPlaintext,
		SessionTimeout:         time.Duration(0),
		SSL:                    kafkaconfig.SSL{KeyPath: ""},
		Topic:                  "",
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ProducerDefaults

	assert.Equal(t, kafkaconfig.Debug{}, config.Debug)
	assert.Equal(t, 10*time.Second, config.HeartbeatInterval)
	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.Equal(t, 0, config.MaxDeliveryRetries)
	assert.Equal(t, 0*time.Second, config.MaxQueueBufferDuration)
	assert.Equal(t, 2097151, config.MaxQueueSizeKBytes)
	assert.Equal(t, 10000000, config.MaxQueueSize)
	assert.EqualValues(t, kafkaconfig.AckLeader, config.RequiredAcks)
	assert.Equal(t, 30*time.Second, config.SessionTimeout)
	assert.Equal(t, kafkaconfig.SSL{}, config.SSL)
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

		"brokers": {
			&kafkaconfig.Producer{Brokers: []string{"1", "2", "3"}},
			&kafka.ConfigMap{"metadata.broker.list": "1,2,3"},
		},

		"brokers (empty)": {
			&kafkaconfig.Producer{Brokers: []string{}},
			&kafka.ConfigMap{},
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

		"logger (skipped)": {
			&kafkaconfig.Producer{Logger: *zap.NewNop()},
			&kafka.ConfigMap{},
		},

		"maxDeliveryRetries": {
			&kafkaconfig.Producer{MaxDeliveryRetries: 10},
			&kafka.ConfigMap{"message.send.max.retries": 10},
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
			&kafkaconfig.Producer{MaxQueueSize: 99},
			&kafka.ConfigMap{"queue.buffering.max.messages": 99},
		},

		"maxQueueSize (empty)": {
			&kafkaconfig.Producer{MaxQueueSize: 0},
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
		valid bool
		cfg   *kafkaconfig.Producer
	}{
		"empty": {
			false,
			&kafkaconfig.Producer{},
		},

		"valid": {
			true,
			&kafkaconfig.Producer{Brokers: []string{"1"}, Topic: "1"},
		},

		"brokers (missing)": {
			false,
			&kafkaconfig.Producer{Topic: "1"},
		},

		"brokers (empty)": {
			false,
			&kafkaconfig.Producer{Brokers: []string{}, Topic: "1"},
		},

		"topic (missing)": {
			false,
			&kafkaconfig.Producer{Brokers: []string{"1"}},
		},

		"topic (empty)": {
			false,
			&kafkaconfig.Producer{Brokers: []string{"1"}, Topic: ""},
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := tt.cfg.ConfigMap()
			if tt.valid {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
			}
		})
	}
}
