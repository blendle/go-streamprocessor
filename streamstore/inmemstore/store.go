package inmemstore

import (
	"reflect"
	"sync"

	"github.com/blendle/go-streamprocessor/v3/stream"
)

// Store hold the in-memory representation of a data storage service.
type Store struct {
	messages []stream.Message
	mutex    sync.RWMutex
}

// New initializes a new store struct.
func New() stream.Store {
	return &Store{messages: make([]stream.Message, 0)}
}

// Add adds a stream.Message to the store.
func (s *Store) Add(msg stream.Message) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.messages = append(s.messages, msg)

	return nil
}

// Del deletes a stream.Message from the store.
func (s *Store) Del(msg stream.Message) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	ks := s.messages
	for i := range s.messages {
		if reflect.DeepEqual(s.messages[i], msg) {
			ks = append(s.messages[:i], s.messages[i+1:]...)
			break
		}
	}

	s.messages = ks

	return nil
}

// Flush clears the store's contents.
func (s *Store) Flush() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.messages = []stream.Message{}

	return nil
}

// Messages returns all messages in the store.
func (s *Store) Messages() []stream.Message {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	return s.messages
}
