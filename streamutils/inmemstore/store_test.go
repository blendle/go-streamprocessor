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

func TestAdd(t *testing.T) {
	t.Parallel()

	store := New()
	m := streammsg.Message{
		Value:     []byte("testValue"),
		Key:       []byte("testKey"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.Add(m)

	msg := store.store[0]

	var tests = []struct {
		expected interface{}
		actual   interface{}
	}{
		{m.Value, msg.Value},
		{m.Key, msg.Key},
		{m.Timestamp, msg.Timestamp},
		{m.Topic, msg.Topic},
		{m.Tags, msg.Tags},
	}

	for _, tt := range tests {
		assert.EqualValues(t, tt.expected, tt.actual)
	}
}

func TestDelete(t *testing.T) {
	t.Parallel()

	store := New()
	m1 := streammsg.Message{
		Value:     []byte("testValue1"),
		Key:       []byte("testKey1"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	m2 := streammsg.Message{
		Value:     []byte("testValue2"),
		Key:       []byte("testKey2"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	m3 := streammsg.Message{
		Value:     []byte("testValue3"),
		Key:       []byte("testKey3"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.store = append(store.store, m1, m2)
	require.Len(t, store.store, 2)

	store.Delete(m1)
	assert.Len(t, store.store, 1)
	assert.Equal(t, m2, store.store[0])

	store.Delete(m3)
	assert.Len(t, store.store, 1)
	assert.Equal(t, m2, store.store[0])

	store.Delete(m2)
	assert.Len(t, store.store, 0)
}

func TestMessages(t *testing.T) {
	t.Parallel()

	store := New()
	m := streammsg.Message{
		Value:     []byte("testValue"),
		Key:       []byte("testKey"),
		Timestamp: time.Unix(0, 0),
		Topic:     "testTopic",
		Tags:      map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.store = append(store.store, m)

	assert.EqualValues(t, m.Value, store.Messages()[0].Value)
}
