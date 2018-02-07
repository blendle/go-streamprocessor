package inmemstore

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
)

func TestNew(t *testing.T) {
	t.Parallel()

	store := New()
	expected := "*inmemstore.Store"
	actual := reflect.TypeOf(store).String()

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestAddMessage(t *testing.T) {
	t.Parallel()

	store := New()
	m := &fakeMessage{
		value:     []byte("testValue"),
		key:       []byte("testKey"),
		timestamp: time.Unix(0, 0),
		topic:     "testTopic",
		offset:    1,
		partition: 2,
		tags:      map[string]string{"test": "value", "test2": "value2"},
	}

	store.AddMessage(m)

	sm, ok := store.store[0].(fakeImplementation)
	if !ok {
		t.Fatal("unable to convert message to correct interface")
	}

	var tests = []struct {
		expected interface{}
		actual   interface{}
	}{
		{m.value, sm.Value()},
		{m.key, sm.Key()},
		{m.timestamp, sm.Timestamp()},
		{m.topic, sm.Topic()},
		{m.offset, sm.Offset()},
		{m.partition, sm.Partition()},
		{m.tags, sm.Tags()},
	}

	for _, tt := range tests {
		if !reflect.DeepEqual(tt.expected, tt.actual) {
			t.Errorf("Expected %v to equal %v", tt.actual, tt.expected)
		}
	}
}

func TestAdd(t *testing.T) {
	t.Parallel()

	store := New()
	m := &fakeMessage{
		value:     []byte("testValue"),
		key:       []byte("testKey"),
		timestamp: time.Unix(0, 0),
		topic:     "testTopic",
		offset:    1,
		partition: 2,
		tags:      map[string]string{"test": "value", "test2": "value2"},
	}

	store.Add(m.key, m.value, m.timestamp, m.topic, m.offset, m.partition, m.tags)

	sm := store.store[0].(fakeImplementation)

	var tests = []struct {
		expected interface{}
		actual   interface{}
	}{
		{m.value, sm.Value()},
		{m.key, sm.Key()},
		{m.timestamp, sm.Timestamp()},
		{m.topic, sm.Topic()},
		{m.offset, sm.Offset()},
		{m.partition, sm.Partition()},
		{m.tags, sm.Tags()},
	}

	for _, tt := range tests {
		if !reflect.DeepEqual(tt.expected, tt.actual) {
			t.Errorf("Expected %v to equal %v", tt.actual, tt.expected)
		}
	}
}

func TestMessages(t *testing.T) {
	t.Parallel()

	store := New()
	m := &fakeMessage{
		value:     []byte("testValue"),
		key:       []byte("testKey"),
		timestamp: time.Unix(0, 0),
		topic:     "testTopic",
		offset:    1,
		partition: 2,
		tags:      map[string]string{"test": "value", "test2": "value2"},
	}

	store.store = append(store.store, m)

	actual := store.Messages()[0].Value()
	expected := m.value

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

type fakeImplementation interface {
	streammsg.Message

	streammsg.KeyReader
	streammsg.TimestampReader
	streammsg.TopicReader
	streammsg.OffsetReader
	streammsg.PartitionReader
	streammsg.TagsReader
}

type fakeMessage struct {
	key       []byte
	value     []byte
	timestamp time.Time
	topic     string
	offset    int64
	partition int32
	tags      map[string]string
}

func (m *fakeMessage) Value() []byte {
	return m.value
}

func (m *fakeMessage) SetValue(v []byte) {}

func (m *fakeMessage) Key() []byte {
	return m.key
}

func (m *fakeMessage) Offset() int64 {
	return m.offset
}

func (m *fakeMessage) Partition() int32 {
	return m.partition
}

func (m *fakeMessage) Tag(v string) string {
	return m.tags[v]
}

func (m *fakeMessage) Tags() map[string]string {
	return m.tags
}

func (m *fakeMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *fakeMessage) Topic() string {
	return m.topic
}

func (m *fakeMessage) Ack() {}
