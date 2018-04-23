package stream_test

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/stretchr/testify/assert"
)

func TestTestMessage(t *testing.T) {
	t.Parallel()

	message := stream.TestMessage(t, "hello", "world")

	assert.Equal(t, "hello", string(message.Key))
	assert.Equal(t, "world", string(message.Value))
	assert.Equal(t, time.Unix(0, 0), message.Timestamp)
	assert.Equal(t, "testTopic", message.Topic)
	assert.Equal(t, int64(12), *message.Offset)
	assert.Equal(t, "testTagValue1", string(message.Tags["testTagKey1"]))
	assert.Equal(t, "testTagValue2", string(message.Tags["testTagKey2"]))
}

func TestTestMessage_DefaultKeyAndValue(t *testing.T) {
	t.Parallel()

	message := stream.TestMessage(t, "", "")

	assert.Equal(t, "testKey", string(message.Key))
	assert.Equal(t, "testValue", string(message.Value))
}
