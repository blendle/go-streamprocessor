package kafkaconfig_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Consumer{
		Brokers:           []string{},
		CommitInterval:    time.Duration(0),
		Debug:             kafkaconfig.Debug{All: true},
		GroupID:           "",
		HeartbeatInterval: time.Duration(0),
		ID:                "",
		InitialOffset:     kafkaconfig.OffsetBeginning,
		Logger:            *zap.NewNop(),
		SessionTimeout:    time.Duration(0),
		SSL:               kafkaconfig.SSL{KeyPath: ""},
		Topics:            []string{},
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ConsumerDefaults

	assert.Equal(t, 5*time.Second, config.CommitInterval)
	assert.Equal(t, kafkaconfig.Debug{}, config.Debug)
	assert.Equal(t, 10*time.Second, config.HeartbeatInterval)
	assert.Equal(t, kafkaconfig.OffsetBeginning, config.InitialOffset)
	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.Equal(t, 30*time.Second, config.SessionTimeout)
	assert.Equal(t, kafkaconfig.SSL{}, config.SSL)
}
