package standardstreamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
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

	expected := "*zap.Logger"
	actual := reflect.TypeOf(config.Logger).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
