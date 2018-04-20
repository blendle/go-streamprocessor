package streammsg_test

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestMessage(t *testing.T) {
	t.Parallel()

	message := streammsg.TestMessage(t, "hello", "world")

	kr, ok := message.(streammsg.KeyReader)
	require.True(t, ok, "unable to convert message to correct interface")

	assert.Equal(t, "hello", string(kr.Key()))
	assert.Equal(t, "world", string(message.Value()))
}

func TestTestMessageWithStruct(t *testing.T) {
	t.Parallel()

	message, str := streammsg.TestMessageWithStruct(t, "hello", "world")

	_, ok := message.(streammsg.KeyReader)
	require.True(t, ok, "unable to convert message to correct interface")

	assert.Equal(t, "hello", string(str.KeyField))
	assert.Equal(t, "world", string(str.ValueField))
	assert.Equal(t, time.Unix(0, 0), str.TimestampField)
	assert.Equal(t, "testTopic", str.TopicField)
	assert.Equal(t, int64(0), str.OffsetField)
	assert.Equal(t, int32(0), str.PartitionField)
	assert.Equal(t, "testTagValue1", string(str.TagsField["testTagKey1"]))
}
