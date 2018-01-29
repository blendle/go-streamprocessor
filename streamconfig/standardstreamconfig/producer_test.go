package standardstreamconfig_test

import (
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"go.uber.org/zap"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = standardstreamconfig.Producer{
		Writer: os.Stdout,
	}
}

func TestProducerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	cc := standardstreamconfig.Client{Logger: logger}
	config := standardstreamconfig.ProducerDefaults(cc)

	expected := logger
	actual := config.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
