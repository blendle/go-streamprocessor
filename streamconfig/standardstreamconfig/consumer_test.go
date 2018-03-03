package standardstreamconfig_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Consumer{
		Logger: *zap.NewNop(),
		Reader: os.Stdin,
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := standardstreamconfig.ConsumerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.EqualValues(t, os.Stdin, config.Reader)
}
