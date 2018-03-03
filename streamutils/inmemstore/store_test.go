package inmemstore

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
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
