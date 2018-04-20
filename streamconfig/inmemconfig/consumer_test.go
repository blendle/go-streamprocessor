package inmemconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Consumer{
		Logger: zap.NewNop(),
		Store:  inmemstore.New(),
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()
	store := inmemstore.New()

	cc := inmemconfig.Client{Logger: logger, Store: store}
	config := inmemconfig.ConsumerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
	assert.EqualValues(t, store, config.Store)
}
