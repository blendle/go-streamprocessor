package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Producer{
		Logger: zap.NewNop(),
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := pubsubconfig.ProducerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
