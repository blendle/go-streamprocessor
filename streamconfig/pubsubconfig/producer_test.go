package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = pubsubconfig.Producer{}
}
