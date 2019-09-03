package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamconfig/pubsubconfig"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Consumer{}
}
