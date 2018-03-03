package standardstreamconfig_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Producer{
		Logger: *zap.NewNop(),
		Writer: os.Stdout,
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := standardstreamconfig.ProducerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.EqualValues(t, os.Stdout, config.Writer)
}
