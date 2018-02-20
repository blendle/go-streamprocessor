package standardstreamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Client{
		Logger: zap.NewNop(),
	}
}

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	config := standardstreamconfig.ClientDefaults()

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
