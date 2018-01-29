package pubsubconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	_ = pubsubconfig.Client{}
	_ = pubsubconfig.Client{
		Logger: zap.NewNop(),
	}
}
