package inmem

import (
	"sort"
	"sync"
)

// DefaultStore is the global default store.
var DefaultStore *Store

// Store hold the in-memory representation of a data storage service.
type Store struct {
	Topics map[string]*Topic
	mutex  sync.RWMutex
}

// Topic contains all the messages
type Topic struct {
	Partitions map[string]*Partition
	mutex      sync.RWMutex
}

// Partition is a single instance of a store, containing messages.
type Partition struct {
	offset   int
	messages map[int][]byte
	mutex    sync.RWMutex
}

// NewStore initializes a new empty in-memory store.
func NewStore() *Store {
	store := &Store{}

	if DefaultStore != nil {
		store = DefaultStore
	}

	if store.Topics == nil {
		store.Topics = make(map[string]*Topic)
	}

	return store
}

// NewTopic returns a new topic, or existing one, if it exists.
func (s *Store) NewTopic(name string) *Topic {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if _, ok := s.Topics[name]; !ok {
		s.Topics[name] = &Topic{Partitions: make(map[string]*Partition)}
	}

	return s.Topics[name]
}

// NewMessage creates a new message in a topic.
func (t *Topic) NewMessage(msg []byte, key []byte) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	p := t.getPartition(key)
	p.store(msg)
}

// Messages returns all messages in all Partitions as a map.
func (t *Topic) Messages() [][]byte {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	msgs := make([][]byte, 0)
	for _, p := range t.Partitions {
		msgs = append(msgs, p.Messages()...)
	}

	return msgs
}

// Messages returns all messages in a partition.
func (p *Partition) Messages() [][]byte {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	msgs := make([][]byte, 0, len(p.messages))

	var keys []int
	for k := range p.messages {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, k := range keys {
		msgs = append(msgs, p.messages[k])
	}

	return msgs
}

func (t *Topic) getPartition(key []byte) *Partition {
	skey := string(key)

	if len(t.Partitions) > 0 {
		if key == nil {
			for _, p := range t.Partitions {
				return p
			}
		} else if _, ok := t.Partitions[skey]; ok {
			return t.Partitions[skey]
		}
	}

	t.Partitions[skey] = newPartition()

	return t.Partitions[skey]
}

func newPartition() *Partition {
	return &Partition{offset: 0, messages: make(map[int][]byte)}
}

func (p *Partition) store(message []byte) {
	p.offset = p.offset + 1
	p.messages[p.offset] = message
}
