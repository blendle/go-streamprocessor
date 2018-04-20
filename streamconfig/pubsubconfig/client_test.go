package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
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

	expected := "*zap.Logger"
	actual := reflect.TypeOf(config.Logger).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
