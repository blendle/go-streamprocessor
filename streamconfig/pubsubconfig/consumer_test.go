package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Consumer{}
}
