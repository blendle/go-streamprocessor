package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Client{
		Logger: zap.NewNop(),
	}
}

func TestClientDefaults(t *testing.T) {
	t.Parallel()

	config := inmemconfig.ClientDefaults()

	expected := "*zap.Logger"
	actual := reflect.TypeOf(config.Logger).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
