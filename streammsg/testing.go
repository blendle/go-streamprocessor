package streammsg

import (
	"testing"
	"time"
)

// TestMessage returns a new message interface with test data to be used during
// testing.
func TestMessage(tb testing.TB, k, v string) Message {
	m, _ := TestMessageWithStruct(tb, k, v)

	return m
}

// TestMessageWithStruct returns a new message interface with test data to be
// used during testing. It also returns the original message struct as the
// second argument.
func TestMessageWithStruct(_ testing.TB, k, v string) (Message, *MessageMock) {
	if k == "" {
		k = "testKey"
	}

	if v == "" {
		v = "testValue"
	}

	m := &MessageMock{
		ValueField:     []byte(v),
		KeyField:       []byte(k),
		TimestampField: time.Unix(0, 0),
		TopicField:     "testTopic",
		OffsetField:    0,
		PartitionField: 0,
		TagsField: map[string][]byte{
			"testTagKey1": []byte("testTagValue1"),
			"testTagKey2": []byte("testTagValue2"),
		},
	}

	return Message(m), m
}
