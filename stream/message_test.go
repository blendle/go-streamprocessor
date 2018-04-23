package stream

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageStruct(t *testing.T) {
	t.Parallel()

	offset := int64(12)
	_ = Message{
		Key:       []byte("testKey"),
		Value:     []byte("testValue"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Offset:    &offset,
		Tags: map[string][]byte{
			"testTagKey1": []byte("testTagValue1"),
			"testTagKey2": []byte("testTagValue2"),
		},
	}
}

func TestSetMessageOpaque(t *testing.T) {
	t.Parallel()

	msg := &Message{}
	err := SetMessageOpaque(msg, "hello world")

	require.NoError(t, err)
	assert.Equal(t, "hello world", msg.opaque.(string))
}

func TestSetMessageOpaque_DisableOverwriting(t *testing.T) {
	t.Parallel()

	msg := &Message{opaque: "hello universe!"}
	err := SetMessageOpaque(msg, "hello world")

	require.Error(t, err)
	assert.Equal(t, "hello universe!", msg.opaque.(string))
}

func TestMessageOpaque(t *testing.T) {
	t.Parallel()

	msg := &Message{opaque: "hello world"}
	opaque := MessageOpqaue(msg)

	assert.Equal(t, "hello world", opaque.(string))
}
