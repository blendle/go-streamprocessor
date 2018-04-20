package standardstreamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	_ = standardstreamconfig.Consumer{}
}

func TestConsumerDefaults(t *testing.T) {
	logger := zap.NewExample()

	cc := standardstreamconfig.Client{Logger: logger}
	config := standardstreamconfig.ConsumerDefaults(cc)

	expected := logger
	actual := config.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
