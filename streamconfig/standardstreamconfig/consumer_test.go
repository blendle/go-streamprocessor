package standardstreamconfig_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Consumer{
		Reader: os.Stdin,
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := standardstreamconfig.ConsumerDefaults

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.EqualValues(t, os.Stdin, config.Reader)
}
