package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Client{
		Logger: zap.NewNop(),
		Store:  inmemstore.New(),
	}
}

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	config := inmemconfig.ClientDefaults()

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
	assert.Equal(t, "*inmemstore.Store", reflect.TypeOf(config.Store).String())
}
