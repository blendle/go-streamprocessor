package kafkaconfig_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	_ = kafkaconfig.Client{}
	_ = kafkaconfig.Client{
		Logger: zap.NewNop(),
	}
}
