package kafkaconfig_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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
		MaxQueueSize:           0,
		RequiredAcks:           kafkaconfig.AckAll,
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
	assert.Equal(t, 10000000, config.MaxQueueSize)
	assert.EqualValues(t, kafkaconfig.AckLeader, config.RequiredAcks)
	assert.Equal(t, 30*time.Second, config.SessionTimeout)
	assert.Equal(t, kafkaconfig.SSL{}, config.SSL)
}
