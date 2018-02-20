package kafkaconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Consumer{}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := kafkaconfig.Client{Logger: logger}
	config := kafkaconfig.ConsumerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
}
