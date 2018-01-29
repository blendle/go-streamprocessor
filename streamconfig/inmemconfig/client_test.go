package inmemconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	_ = inmemconfig.Client{
		Logger: zap.NewNop(),
	}
}
