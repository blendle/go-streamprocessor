package standardstreamconfig_test

import (
	"os"
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

	assert.EqualValues(t, os.Stdin, config.Reader)
}
