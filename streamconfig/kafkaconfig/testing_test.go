package kafkaconfig

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutils/testutils"
	"github.com/stretchr/testify/assert"
)

func TestTestBrokerAddress(t *testing.T) {
	assert.Equal(t, "127.0.0.1:9092", TestBrokerAddress)
}

func TestTestConsumer(t *testing.T) {
	t.Parallel()

	x := testutils.TimeoutMultiplier
	c := TestConsumer(t)

	assert.Equal(t, []string{TestBrokerAddress}, c.Brokers)
	assert.Equal(t, time.Duration(500*x)*time.Millisecond, c.CommitInterval)
	assert.Equal(t, "testGroup", c.GroupID)
	assert.Equal(t, time.Duration(150*x)*time.Millisecond, c.HeartbeatInterval)
	assert.Equal(t, "testConsumer", c.ID)
	assert.Equal(t, OffsetBeginning, c.InitialOffset)
	assert.Equal(t, ProtocolPlaintext, c.SecurityProtocol)
	assert.Equal(t, time.Duration(1000*x)*time.Millisecond, c.SessionTimeout)
	assert.Equal(t, []string{"testTopic"}, c.Topics)
}
