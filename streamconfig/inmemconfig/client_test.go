package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
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

	expected := "*zap.Logger"
	actual := reflect.TypeOf(config.Logger).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	expected = "*inmemstore.Store"
	actual = reflect.TypeOf(config.Store).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
