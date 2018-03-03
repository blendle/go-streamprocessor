package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Consumer{
		Logger: *zap.NewNop(),
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := pubsubconfig.ConsumerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
}
