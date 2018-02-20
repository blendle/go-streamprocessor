package kafkaconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = kafkaconfig.Client{
		Logger: zap.NewNop(),
	}
}

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	config := kafkaconfig.ClientDefaults()

	assert.Equal(t, "*zap.Logger", reflect.TypeOf(config.Logger).String())
}
