package kafkaconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTestConsumer(t *testing.T) {
	t.Parallel()

	c := TestConsumer(t)

	assert.Equal(t, []string{TestBrokerAddress}, c.Brokers)
	assert.Equal(t, time.Duration(500*time.Millisecond), c.CommitInterval)
	assert.Equal(t, "testGroup", c.GroupID)
	assert.Equal(t, time.Duration(150*time.Millisecond), c.HeartbeatInterval)
	assert.Equal(t, "testConsumer", c.ID)
	assert.Equal(t, OffsetBeginning, c.OffsetInitial)
	assert.Equal(t, ProtocolPlaintext, c.SecurityProtocol)
	assert.Equal(t, 1*time.Second, c.SessionTimeout)
	assert.Equal(t, []string{"testTopic"}, c.Topics)
}
