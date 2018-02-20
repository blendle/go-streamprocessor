package inmemclient

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
)

type testMessage interface {
	streammsg.Message

	streammsg.KeyReader
	streammsg.KeyWriter
	streammsg.TimestampReader
	streammsg.TimestampWriter
	streammsg.TopicReader
	streammsg.TopicWriter
	streammsg.OffsetReader
	streammsg.PartitionReader
	streammsg.TagsReader
	streammsg.TagsWriter
	streammsg.Nacker
}

func TestMessage(t *testing.T) {
	t.Parallel()

	_, _ = newMessage()
}

func TestMessage_Value(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, "testValue", string(msg.Value()))
}

func TestMessage_SetValue(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()

	msg.SetValue([]byte("testValue2"))
	assert.Equal(t, "testValue2", string(str.value))
}

func TestMessage_Key(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, "testKey", string(msg.Key()))
}

func TestMessage_SetKey(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()

	msg.SetKey([]byte("testKey2"))
	assert.Equal(t, "testKey2", string(str.key))
}

func TestMessage_Timestamp(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, time.Unix(0, 0), msg.Timestamp())
}

func TestMessage_SetTimestamp(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()

	msg.SetTimestamp(time.Unix(10, 0))
	assert.Equal(t, time.Unix(10, 0), str.timestamp)
}

func TestMessage_Topic(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, "testTopic", msg.Topic())
}

func TestMessage_SetTopic(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()

	msg.SetTopic("testTopic2")
	assert.Equal(t, "testTopic2", str.topic)
}

func TestMessage_Offset(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, int64(1), msg.Offset())
}

func TestMessage_Partition(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, int32(2), msg.Partition())
}

func TestMessage_Tags(t *testing.T) {
	t.Parallel()

	expected := map[string][]byte{"test": []byte("value"), "test2": []byte("value2")}

	_, msg := newMessage()
	assert.EqualValues(t, expected, msg.Tags())
}

func TestMessage_Tag(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.EqualValues(t, []byte("value2"), msg.Tag("test2"))
}

func TestMessage_SetTags(t *testing.T) {
	t.Parallel()

	expected := map[string][]byte{"test3": []byte("value3")}
	str, msg := newMessage()

	msg.SetTags(expected)
	assert.EqualValues(t, expected, str.tags)
}

func TestMessage_SetTag(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	expected := map[string][]byte{
		"test":  []byte("value"),
		"test2": []byte("value2"),
		"test3": []byte("value3"),
	}

	msg.SetTag("test3", expected["test3"])
	assert.EqualValues(t, expected, str.tags)
}

func TestMessage_RemoveTag(t *testing.T) {
	t.Parallel()

	expected := map[string][]byte{"test2": []byte("value2")}
	str, msg := newMessage()

	msg.RemoveTag("test")
	assert.EqualValues(t, expected, str.tags)
}

func newMessage() (*message, testMessage) {
	m := &message{
		value:     []byte("testValue"),
		key:       []byte("testKey"),
		timestamp: time.Unix(0, 0),
		topic:     "testTopic",
		offset:    1,
		partition: 2,
		tags: map[string][]byte{
			"test":  []byte("value"),
			"test2": []byte("value2"),
		},
	}

	return m, testMessage(m)
}
