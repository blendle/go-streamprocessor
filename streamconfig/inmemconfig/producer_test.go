package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/v3/streamstore/inmemstore"
	"github.com/stretchr/testify/assert"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Producer{
		Store: inmemstore.New(),
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	config := inmemconfig.ProducerDefaults

	assert.Equal(t, "*inmemstore.Store", reflect.TypeOf(config.Store).String())
}
