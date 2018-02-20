package inmemclient

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
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

	actual := string(msg.Value())
	expected := "testValue"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetValue(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetValue([]byte("testValue2"))

	actual := string(str.value)
	expected := "testValue2"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Key(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := string(msg.Key())
	expected := "testKey"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetKey(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetKey([]byte("testKey2"))

	actual := string(str.key)
	expected := "testKey2"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Timestamp(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := msg.Timestamp()
	expected := time.Unix(0, 0)

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetTimestamp(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetTimestamp(time.Unix(10, 0))

	actual := str.timestamp
	expected := time.Unix(10, 0)

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Topic(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := msg.Topic()
	expected := "testTopic"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetTopic(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetTopic("testTopic2")

	actual := str.topic
	expected := "testTopic2"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Offset(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := msg.Offset()
	expected := int64(1)

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Partition(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := msg.Partition()
	expected := int32(2)

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Tags(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := msg.Tags()
	expected := map[string][]byte{"test": []byte("value"), "test2": []byte("value2")}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_Tag(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()

	actual := string(msg.Tag("test2"))
	expected := "value2"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetTags(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetTags(map[string][]byte{"test3": []byte("value3")})

	actual := str.tags
	expected := map[string][]byte{"test3": []byte("value3")}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetTag(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetTag("test3", "value3")

	actual := str.tags
	expected := map[string][]byte{
		"test":  []byte("value"),
		"test2": []byte("value2"),
		"test3": []byte("value3"),
	}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_RemoveTag(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.RemoveTag("test")

	actual := str.tags
	expected := map[string][]byte{"test2": []byte("value2")}

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
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
