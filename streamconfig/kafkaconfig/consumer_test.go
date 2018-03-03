package kafkaconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Consumer{
		Logger: *zap.NewNop(),
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ConsumerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
}
