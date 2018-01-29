package streamconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Client{}
}

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Consumer{}
}

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = streamconfig.Producer{}
}
