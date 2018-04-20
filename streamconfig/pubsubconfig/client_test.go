package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Client{
		Logger: zap.NewNop(),
	}
}

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	config := pubsubconfig.ClientDefaults()

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
