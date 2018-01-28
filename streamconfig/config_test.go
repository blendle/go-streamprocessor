package streamconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestClient(t *testing.T) {
	_ = streamconfig.Client{}
}

func TestConsumer(t *testing.T) {
	_ = streamconfig.Consumer{}
}

func TestProducer(t *testing.T) {
	_ = streamconfig.Producer{}
}
