package streamconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Consumer{}
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Producer{}
}
