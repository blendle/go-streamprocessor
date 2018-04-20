package kafkaconfig

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutils/testutils"
)

// TestBrokerAddress is the address used to connect to the testing broker.
// This defaults to 127.0.0.1:9092, but can be overwritten if desired.
var TestBrokerAddress = "127.0.0.1:9092"

// TestConsumer returns a kafkaconfig.Consumer struct with its options tweaked
// for testing purposes.
func TestConsumer(tb testing.TB) Consumer {
	config := ConsumerDefaults

	config.Brokers = []string{TestBrokerAddress}
	config.CommitInterval = time.Duration(500*testutils.TimeoutMultiplier) * time.Millisecond
	config.GroupID = "testGroup"
	config.HeartbeatInterval = time.Duration(150*testutils.TimeoutMultiplier) * time.Millisecond
	config.ID = "testConsumer"
	config.InitialOffset = OffsetBeginning
	config.SecurityProtocol = ProtocolPlaintext
	config.SessionTimeout = time.Duration(1000*testutils.TimeoutMultiplier) * time.Millisecond
	config.Topics = []string{"testTopic"}

	if testing.Verbose() {
		config.Debug.All = true
	}

	return config
}
