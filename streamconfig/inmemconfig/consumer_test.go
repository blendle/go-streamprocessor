package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/v3/streamstore/inmemstore"
	"github.com/stretchr/testify/assert"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Consumer{
		Store:       inmemstore.New(),
		ConsumeOnce: false,
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	config := inmemconfig.ConsumerDefaults

	assert.Equal(t, "*inmemstore.Store", reflect.TypeOf(config.Store).String())
	assert.Equal(t, true, config.ConsumeOnce)
}
