package kafkaconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Producer{
		Logger: *zap.NewNop(),
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ProducerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
}
