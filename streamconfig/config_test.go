package streamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Consumer{
		Inmem:          inmemconfig.Consumer{},
		Kafka:          kafkaconfig.Consumer{},
		Pubsub:         pubsubconfig.Consumer{},
		Standardstream: standardstreamconfig.Consumer{},
		Logger:         *zap.NewNop(),
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ConsumerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Producer{
		Inmem:          inmemconfig.Producer{},
		Kafka:          kafkaconfig.Producer{},
		Pubsub:         pubsubconfig.Producer{},
		Standardstream: standardstreamconfig.Producer{},
		Logger:         *zap.NewNop(),
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := streamconfig.ProducerDefaults

	assert.Equal(t, "zap.Logger", reflect.TypeOf(config.Logger).String())
}
