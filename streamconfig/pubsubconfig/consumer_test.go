package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	_ = pubsubconfig.Consumer{}
}

func TestConsumerDefaults(t *testing.T) {
	logger := zap.NewExample()

	cc := pubsubconfig.Client{Logger: logger}
	config := pubsubconfig.ConsumerDefaults(cc)

	expected := logger
	actual := config.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
