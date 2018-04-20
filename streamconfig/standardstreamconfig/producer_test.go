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
		Writer: os.Stdout,
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := standardstreamconfig.Client{Logger: logger}
	config := standardstreamconfig.ProducerDefaults(cc)

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
