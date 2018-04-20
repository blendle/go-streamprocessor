package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Consumer{}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := pubsubconfig.Client{Logger: logger}
	config := pubsubconfig.ConsumerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
}
