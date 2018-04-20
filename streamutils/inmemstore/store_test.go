package inmemstore

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	store := New()

	assert.Equal(t, "*inmemstore.Store", reflect.TypeOf(store).String())
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
		tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.AddMessage(m)

	sm, ok := store.store[0].(fakeImplementation)
	require.True(t, ok, "unable to convert message to correct interface")

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
		assert.EqualValues(t, tt.expected, tt.actual)
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
		tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
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
		assert.EqualValues(t, tt.expected, tt.actual)
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
		tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.store = append(store.store, m)

	assert.EqualValues(t, m.value, store.Messages()[0].Value())
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
	tags      map[string][]byte
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

func (m *fakeMessage) Tag(v string) []byte {
	return m.tags[v]
}

func (m *fakeMessage) Tags() map[string][]byte {
	return m.tags
}

func (m *fakeMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *fakeMessage) Topic() string {
	return m.topic
}

func (m *fakeMessage) Ack() error {
	return nil
}
