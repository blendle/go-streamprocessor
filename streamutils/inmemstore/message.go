package inmemstore

import "time"

// message is the format of the messages stored in the store.
type message struct {
	key       []byte
	value     []byte
	timestamp time.Time
	topic     string
	offset    int64
	partition int32
	tags      map[string]string
}

// Key returns the key of the message.
func (m message) Key() []byte {
	return m.key
}

// SetKey sets the key of the message.
func (m *message) SetKey(v []byte) {
	m.key = v
}

// Value returns the value of the message.
func (m message) Value() []byte {
	return m.value
}

// SetValue sets the value of the message.
func (m *message) SetValue(v []byte) {
	m.value = v
}

// Timestamp returns the timestamp of the message.
func (m message) Timestamp() time.Time {
	return m.timestamp
}

// SetTimestamp sets the timestamp of the message.
func (m *message) SetTimestamp(v time.Time) {
	m.timestamp = v
}

// Topic returns the topic of the message.
func (m message) Topic() string {
	return m.topic
}

// SetTopic sets the topic of the message.
func (m *message) SetTopic(v string) {
	m.topic = v
}

// Offset returns the offset of the message.
func (m message) Offset() int64 {
	return m.offset
}

// Partition returns the partition of the message.
func (m message) Partition() int32 {
	return m.partition
}

// Tags returns the tags of the message.
func (m message) Tags() map[string]string {
	return m.tags
}

// Tag returns a single tag of the message.
func (m message) Tag(v string) string {
	return m.tags[v]
}

// SetTags sets the tags of the message.
func (m *message) SetTags(v map[string]string) {
	m.tags = v
}

// SetTag sets a single tag of the message.
func (m *message) SetTag(k, v string) {
	m.tags[k] = v
}

// RemoveTag removes a single tag from the message.
func (m *message) RemoveTag(v string) {
	delete(m.tags, v)
}

// Ack is a no-op implementation of the streammsg.Acker interface.
func (m *message) Ack() {}

// Nack is a no-op implementation of the streammsg.Nacker interface.
func (m *message) Nack() {}
