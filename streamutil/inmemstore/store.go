package inmemstore

import (
	"reflect"
	"sync"

	"github.com/blendle/go-streamprocessor/stream"
)

// Store hold the in-memory representation of a data storage service.
type Store struct {
	store []stream.Message
	mutex sync.RWMutex
}

// New initializes a new store struct.
func New() *Store {
	return &Store{store: make([]stream.Message, 0)}
}

// Add adds a stream.Message to the store.
func (s *Store) Add(msg stream.Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.store = append(s.store, msg)
}

// Delete deletes a stream.Message from the store.
func (s *Store) Delete(msg stream.Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ks := s.store
	for i := range s.store {
		if reflect.DeepEqual(s.store[i], msg) {
			ks = append(s.store[:i], s.store[i+1:]...)
			break
		}
	}
	s.store = ks
}

// Messages returns all messages in the store.
func (s *Store) Messages() []stream.Message {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.store
}
