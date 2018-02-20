package kafkaconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Producer{}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := kafkaconfig.Client{Logger: logger}
	config := kafkaconfig.ProducerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
}
