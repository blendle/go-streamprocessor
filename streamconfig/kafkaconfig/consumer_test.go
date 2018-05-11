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

var consumerDefaults = map[string]interface{}{
	"go.events.channel.enable":        true,
	"enable.partition.eof":            false,
	"enable.auto.commit":              true,
	"enable.auto.offset.store":        false,
	"go.application.rebalance.enable": true,
}

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Consumer{
		Brokers:             []string{},
		CommitInterval:      time.Duration(0),
		Debug:               kafkaconfig.Debug{All: true},
		GroupID:             "",
		HeartbeatInterval:   time.Duration(0),
		ID:                  "",
		MaxInFlightRequests: 0,
		OffsetInitial:       kafkaconfig.OffsetBeginning,
		OffsetDefault:       &[]int64{5}[0],
		SecurityProtocol:    kafkaconfig.ProtocolPlaintext,
		SessionTimeout:      time.Duration(0),
		SSL:                 kafkaconfig.SSL{KeyPath: ""},
		Topics:              []string{},
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ConsumerDefaults

	assert.Equal(t, 5*time.Second, config.CommitInterval)
	assert.Equal(t, kafkaconfig.Debug{}, config.Debug)
	assert.Equal(t, 1*time.Second, config.HeartbeatInterval)
	assert.Equal(t, 1000000, config.MaxInFlightRequests)
	assert.Equal(t, kafkaconfig.OffsetBeginning, config.OffsetInitial)
	assert.Equal(t, 30*time.Second, config.SessionTimeout)
	assert.Equal(t, kafkaconfig.SSL{}, config.SSL)
}

func TestConsumer_ConfigMap(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		cfg *kafkaconfig.Consumer
		cm  *kafka.ConfigMap
	}{
		"empty": {
			&kafkaconfig.Consumer{},
			&kafka.ConfigMap{},
		},

		"brokers": {
			&kafkaconfig.Consumer{Brokers: []string{"1", "2", "3"}},
			&kafka.ConfigMap{"metadata.broker.list": "1,2,3"},
		},

		"brokers (empty)": {
			&kafkaconfig.Consumer{Brokers: []string{}},
			&kafka.ConfigMap{},
		},

		"commitInterval": {
			&kafkaconfig.Consumer{CommitInterval: 10 * time.Millisecond},
			&kafka.ConfigMap{"auto.commit.interval.ms": 10},
		},

		"commitInterval (seconds)": {
			&kafkaconfig.Consumer{CommitInterval: 1 * time.Second},
			&kafka.ConfigMap{"auto.commit.interval.ms": 1000},
		},

		"commitInterval (empty)": {
			&kafkaconfig.Consumer{CommitInterval: 0 * time.Second},
			&kafka.ConfigMap{},
		},

		"debug (all)": {
			&kafkaconfig.Consumer{Debug: kafkaconfig.Debug{All: true, Fetch: true}},
			&kafka.ConfigMap{"debug": "all"},
		},

		"debug (multiple)": {
			&kafkaconfig.Consumer{Debug: kafkaconfig.Debug{Fetch: true, CGRP: true, Broker: false}},
			&kafka.ConfigMap{"debug": "cgrp,fetch"},
		},

		"debug (empty)": {
			&kafkaconfig.Consumer{Debug: kafkaconfig.Debug{}},
			&kafka.ConfigMap{},
		},

		"groupID": {
			&kafkaconfig.Consumer{GroupID: "hello"},
			&kafka.ConfigMap{"group.id": "hello"},
		},

		"groupID (empty)": {
			&kafkaconfig.Consumer{GroupID: ""},
			&kafka.ConfigMap{},
		},

		"heartbeatInterval": {
			&kafkaconfig.Consumer{HeartbeatInterval: 10 * time.Millisecond},
			&kafka.ConfigMap{"heartbeat.interval.ms": 10},
		},

		"heartbeatInterval (minutes)": {
			&kafkaconfig.Consumer{HeartbeatInterval: 2 * time.Minute},
			&kafka.ConfigMap{"heartbeat.interval.ms": 120000},
		},

		"heartbeatInterval (empty)": {
			&kafkaconfig.Consumer{HeartbeatInterval: 0 * time.Millisecond},
			&kafka.ConfigMap{},
		},

		"ID": {
			&kafkaconfig.Consumer{ID: "hello"},
			&kafka.ConfigMap{"client.id": "hello"},
		},

		"ID (empty)": {
			&kafkaconfig.Consumer{ID: ""},
			&kafka.ConfigMap{},
		},

		"maxInFlightRequests": {
			&kafkaconfig.Consumer{MaxInFlightRequests: 10},
			&kafka.ConfigMap{"max.in.flight.requests.per.connection": 10},
		},

		"offsetDefault": {
			&kafkaconfig.Consumer{OffsetDefault: &[]int64{12}[0]},
			&kafka.ConfigMap{},
		},

		"offsetInitial (end)": {
			&kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetEnd},
			&kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "end"}},
		},

		"offsetInitial (beginning)": {
			&kafkaconfig.Consumer{OffsetInitial: kafkaconfig.OffsetBeginning},
			&kafka.ConfigMap{"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "beginning"}},
		},

		"securityProtocol (plaintext)": {
			&kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolPlaintext},
			&kafka.ConfigMap{"security.protocol": "plaintext"},
		},

		"securityProtocol (SSL)": {
			&kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSSL},
			&kafka.ConfigMap{"security.protocol": "ssl"},
		},

		"securityProtocol (SASL plaintext)": {
			&kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSASLPlaintext},
			&kafka.ConfigMap{"security.protocol": "sasl_plaintext"},
		},

		"securityProtocol (SASL SSL)": {
			&kafkaconfig.Consumer{SecurityProtocol: kafkaconfig.ProtocolSASLSSL},
			&kafka.ConfigMap{"security.protocol": "sasl_ssl"},
		},

		"sessionTimeout": {
			&kafkaconfig.Consumer{SessionTimeout: 2 * time.Millisecond},
			&kafka.ConfigMap{"session.timeout.ms": 2},
		},

		"sessionTimeout (seconds)": {
			&kafkaconfig.Consumer{SessionTimeout: 10 * time.Second},
			&kafka.ConfigMap{"session.timeout.ms": 10000},
		},

		"sessionTimeout (invalid)": {
			// Even though this value is an invalid configuration value, we set it
			// anyway, and let the broker crash hard on boot.
			&kafkaconfig.Consumer{SessionTimeout: -1 * time.Second},
			&kafka.ConfigMap{"session.timeout.ms": -1000},
		},

		"sessionTimeout (empty)": {
			&kafkaconfig.Consumer{SessionTimeout: 0 * time.Second},
			&kafka.ConfigMap{},
		},

		"SSL (empty)": {
			&kafkaconfig.Consumer{SSL: kafkaconfig.SSL{}},
			&kafka.ConfigMap{},
		},

		"SSL (single)": {
			&kafkaconfig.Consumer{SSL: kafkaconfig.SSL{CAPath: "/tmp"}},
			&kafka.ConfigMap{"ssl.ca.location": "/tmp"},
		},

		"SSL (all)": {
			&kafkaconfig.Consumer{SSL: kafkaconfig.SSL{
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

		"topics (skipped)": {
			&kafkaconfig.Consumer{Topics: []string{"a", "b"}},
			&kafka.ConfigMap{},
		},

		"multiple": {
			&kafkaconfig.Consumer{
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

			// The above table only compares the non-default values for readability.
			// In this loop, we validate that the default values are set as expected,
			// and then add them to the config, to make the rest of the validation not
			// choke on these extra configuration keys.
			for k, v := range consumerDefaults {
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

func TestConsumer_ConfigMap_Validation(t *testing.T) {
	t.Parallel()

	var tests = map[string]struct {
		err error
		cfg *kafkaconfig.Consumer
	}{
		"empty": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Consumer{},
		},

		"valid": {
			nil,
			&kafkaconfig.Consumer{Brokers: []string{"1"}, Topics: []string{"1"}, GroupID: "1"},
		},

		"brokers (missing)": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Consumer{Topics: []string{"1"}, GroupID: "1"},
		},

		"brokers (empty)": {
			errors.New("required config Kafka.Brokers empty"),
			&kafkaconfig.Consumer{Brokers: []string{}, Topics: []string{"1"}, GroupID: "1"},
		},

		"topics (missing)": {
			errors.New("required config Kafka.Topics empty"),
			&kafkaconfig.Consumer{Brokers: []string{"1"}, GroupID: "1"},
		},

		"topics (empty)": {
			errors.New("required config Kafka.Topics empty"),
			&kafkaconfig.Consumer{Brokers: []string{"1"}, Topics: []string{}, GroupID: "1"},
		},

		"topics (empty value)": {
			errors.New("empty value detected in config Kafka.Topics"),
			&kafkaconfig.Consumer{Brokers: []string{"1"}, Topics: []string{""}, GroupID: "1"},
		},

		"groupID (missing)": {
			errors.New("required config Kafka.GroupID missing"),
			&kafkaconfig.Consumer{Brokers: []string{"1"}, Topics: []string{"1"}},
		},

		"groupID (empty)": {
			errors.New("required config Kafka.GroupID missing"),
			&kafkaconfig.Consumer{Brokers: []string{"1"}, GroupID: "", Topics: []string{"1"}},
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
