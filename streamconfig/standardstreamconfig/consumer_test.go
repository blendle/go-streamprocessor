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
		Reader: os.Stdin,
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := standardstreamconfig.Client{Logger: logger}
	config := standardstreamconfig.ConsumerDefaults(cc)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
