package standardstreamconfig_test

import (
	"os"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Producer{
		Writer: os.Stdout,
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := standardstreamconfig.ProducerDefaults

	assert.EqualValues(t, os.Stdout, config.Writer)
}
