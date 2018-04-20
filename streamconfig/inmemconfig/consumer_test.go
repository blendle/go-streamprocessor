package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	_ = inmemconfig.Consumer{}
}

func TestConsumerDefaults(t *testing.T) {
	logger := zap.NewExample()

	cc := inmemconfig.Client{Logger: logger}
	config := inmemconfig.ConsumerDefaults(cc)

	expected := logger
	actual := config.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
