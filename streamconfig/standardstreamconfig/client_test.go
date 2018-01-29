package standardstreamconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	_ = standardstreamconfig.Client{}
	_ = standardstreamconfig.Client{
		Logger: zap.NewNop(),
	}
}
