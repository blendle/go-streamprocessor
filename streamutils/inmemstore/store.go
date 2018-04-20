package inmemstore

import (
	"sync"
	"time"

	"github.com/blendle/go-streamprocessor/streammsg"
)

// Store hold the in-memory representation of a data storage service.
type Store struct {
	store []streammsg.Message
	mutex sync.RWMutex
}

// New initializes a new store struct.
func New() *Store {
	return &Store{store: make([]streammsg.Message, 0)}
}

// AddMessage adds a streammsg.Message to the store.
func (s *Store) AddMessage(msg streammsg.Message) {
	m := &message{}

	if v, ok := msg.(streammsg.ValueReader); ok {
		m.value = v.Value()
	}

	if v, ok := msg.(streammsg.KeyReader); ok {
		m.key = v.Key()
	}

	if v, ok := msg.(streammsg.TopicReader); ok {
		m.topic = v.Topic()
	}

	if v, ok := msg.(streammsg.TimestampReader); ok {
		m.timestamp = v.Timestamp()
	}

	if v, ok := msg.(streammsg.OffsetReader); ok {
		m.offset = v.Offset()
	}

	if v, ok := msg.(streammsg.PartitionReader); ok {
		m.partition = v.Partition()
	}

	if v, ok := msg.(streammsg.TagsReader); ok {
		m.tags = v.Tags()
	}

	s.Add(m.key, m.value, m.timestamp, m.topic, m.offset, m.partition, m.tags)
}

// Add adds a single message to the store.
func (s *Store) Add(k, v []byte, t time.Time, tp string, o int64, p int32, tg map[string]string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	m := &message{key: k, value: v, timestamp: t, topic: tp, offset: o, partition: p, tags: tg}
	s.store = append(s.store, streammsg.Message(m))
}

// Messages returns all messages in the store.
func (s *Store) Messages() []streammsg.Message {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.store
}
