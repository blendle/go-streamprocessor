package inmemconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Producer{
		Logger: zap.NewNop(),
		Store:  inmemstore.New(),
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()
	store := inmemstore.New()

	cc := inmemconfig.Client{Logger: logger, Store: store}
	config := inmemconfig.ProducerDefaults(cc)

	assert.EqualValues(t, logger, config.Logger)
	assert.EqualValues(t, store, config.Store)
}
