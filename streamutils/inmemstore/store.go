package inmemstore

import (
	"reflect"
	"sync"

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

// Add adds a streammsg.Message to the store.
func (s *Store) Add(msg streammsg.Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.store = append(s.store, msg)
}

// Delete deletes a streammsg.Message from the store.
func (s *Store) Delete(msg streammsg.Message) {
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
func (s *Store) Messages() []streammsg.Message {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.store
}
