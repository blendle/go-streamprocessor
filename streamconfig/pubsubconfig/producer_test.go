package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Producer{}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := pubsubconfig.Client{Logger: logger}
	config := pubsubconfig.ProducerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
}
