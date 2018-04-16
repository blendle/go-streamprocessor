package kafkaconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTestBrokerAddress(t *testing.T) {
	assert.Equal(t, "127.0.0.1:9092", TestBrokerAddress)
}
