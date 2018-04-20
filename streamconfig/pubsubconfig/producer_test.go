package pubsubconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	_ = pubsubconfig.Producer{}
}

func TestProducerDefaults(t *testing.T) {
	logger := zap.NewExample()

	cc := pubsubconfig.Client{Logger: logger}
	config := pubsubconfig.ProducerDefaults(cc)

	expected := logger
	actual := config.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
