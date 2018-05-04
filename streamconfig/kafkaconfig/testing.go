package kafkaconfig

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamutil/testutil"
)

// TestBrokerAddress is the address used to connect to the testing broker.
// This defaults to 127.0.0.1:9092, but can be overwritten if desired.
var TestBrokerAddress = "127.0.0.1:9092"

// TestConsumer returns a kafkaconfig.Consumer struct with its options tweaked
// for testing purposes.
func TestConsumer(tb testing.TB) Consumer {
	config := ConsumerDefaults

	config.Brokers = []string{TestBrokerAddress}
	config.CommitInterval = 500 * time.Millisecond
	config.GroupID = "testGroup"
	config.HeartbeatInterval = 150 * time.Millisecond
	config.ID = "testConsumer"
	config.OffsetInitial = OffsetBeginning
	config.SecurityProtocol = ProtocolPlaintext
	config.SessionTimeout = 1 * time.Second
	config.Topics = []string{"testTopic"}

	if testutil.Verbose(tb) {
		config.Debug.CGRP = true
		config.Debug.Topic = true
	}

	return config
}
