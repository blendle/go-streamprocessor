package stream

import (
	"testing"
	"time"
)

// TestMessage returns a new message with test data to be used during testing.
func TestMessage(_ testing.TB, k, v string) Message {
	if k == "" {
		k = "testKey"
	}

	if v == "" {
		v = "testValue"
	}

	offset := int64(12)
	m := Message{
		Value:         []byte(v),
		Key:           []byte(k),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Offset:        &offset,
		Tags: map[string][]byte{
			"testTagKey1": []byte("testTagValue1"),
			"testTagKey2": []byte("testTagValue2"),
		},
	}

	return m
}
