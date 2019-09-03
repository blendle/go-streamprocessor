package inmemstore

import (
	"reflect"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/v3/stream"
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

	store := &Store{messages: make([]stream.Message, 0)}
	m := stream.Message{
		Value:         []byte("testValue"),
		Key:           []byte("testKey"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	err := store.Add(m)
	require.NoError(t, err)

	msg := store.messages[0]
	assert.EqualValues(t, m, msg)
}

func TestDel(t *testing.T) {
	t.Parallel()

	store := &Store{messages: make([]stream.Message, 0)}
	m1 := stream.Message{
		Value:         []byte("testValue1"),
		Key:           []byte("testKey1"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	m2 := stream.Message{
		Value:         []byte("testValue2"),
		Key:           []byte("testKey2"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	m3 := stream.Message{
		Value:         []byte("testValue3"),
		Key:           []byte("testKey3"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.messages = append(store.messages, m1, m2)
	require.Len(t, store.messages, 2)

	err := store.Del(m1)
	require.NoError(t, err)
	assert.Len(t, store.messages, 1)
	assert.Equal(t, m2, store.messages[0])

	err = store.Del(m3)
	require.NoError(t, err)
	assert.Len(t, store.messages, 1)
	assert.Equal(t, m2, store.messages[0])

	err = store.Del(m2)
	require.NoError(t, err)
	assert.Len(t, store.messages, 0)
}

func TestFlush(t *testing.T) {
	t.Parallel()

	store := &Store{messages: make([]stream.Message, 0)}
	m := stream.Message{
		Value:         []byte("testValue"),
		Key:           []byte("testKey"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	err := store.Add(m)
	require.NoError(t, err)

	err = store.Add(m)
	require.NoError(t, err)

	require.Len(t, store.messages, 2)
	require.NoError(t, store.Flush())
	assert.Len(t, store.messages, 0)
}

func TestMessages(t *testing.T) {
	t.Parallel()

	store := &Store{messages: make([]stream.Message, 0)}
	m := stream.Message{
		Value:         []byte("testValue"),
		Key:           []byte("testKey"),
		Timestamp:     time.Unix(0, 0),
		ConsumerTopic: "testConsumerTopic",
		ProducerTopic: "testProducerTopic",
		Tags:          map[string][]byte{"test": []byte("value"), "test2": []byte("value2")},
	}

	store.messages = append(store.messages, m)

	assert.EqualValues(t, m.Value, store.Messages()[0].Value)
}
