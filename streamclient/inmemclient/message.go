package inmemclient

import "time"

// The following interfaces are implemented by inmemclient:
//
//   streammsg.Message
// 	 streammsg.KeyReader
// 	 streammsg.KeyWriter
// 	 streammsg.TimestampReader
// 	 streammsg.TimestampWriter
// 	 streammsg.TopicReader
// 	 streammsg.TopicWriter
// 	 streammsg.OffsetReader
// 	 streammsg.PartitionReader
// 	 streammsg.TagsReader
// 	 streammsg.TagsWriter
// 	 streammsg.Nacker

type message struct {
	value     []byte
	key       []byte
	timestamp time.Time
	topic     string
	offset    int64
	partition int32
	tags      map[string]string
}

// Value returns the value of the message.
func (m message) Value() []byte {
	return m.value
}

// SetValue sets the value of the message.
func (m *message) SetValue(v []byte) {
	m.value = v
}

// Key returns the value of the key.
func (m message) Key() []byte {
	return m.key
}

// SetKey sets the value of the key.
func (m *message) SetKey(k []byte) {
	m.key = k
}

// Timestamp returns the value of the timestamp.
func (m message) Timestamp() time.Time {
	return m.timestamp
}

// SetTimestamp sets the value of the timestamp.
func (m *message) SetTimestamp(t time.Time) {
	m.timestamp = t
}

// Topic returns the value of the topic.
func (m message) Topic() string {
	return m.topic
}

// SetTopic sets the value of the topic.
func (m *message) SetTopic(t string) {
	m.topic = t
}

// Offset returns the value of the offset.
func (m message) Offset() int64 {
	return m.offset
}

// Partition returns the value of the partition.
func (m message) Partition() int32 {
	return m.partition
}

// Tags returns all the tags.
func (m message) Tags() map[string]string {
	return m.tags
}

// Tag returns a single tag.
func (m message) Tag(k string) string {
	return m.tags[k]
}

// SetTags sets the value of tags.
func (m *message) SetTags(t map[string]string) {
	m.tags = t
}

// SetTag sets the value of a single tag.
func (m *message) SetTag(k, v string) {
	m.tags[k] = v
}

// RemoveTag removes a single tag.
func (m *message) RemoveTag(k string) {
	delete(m.tags, k)
}

// Ack is a no-op implementation of the required interface.
func (m *message) Ack() {}

// Nack is a no-op implementation of the required interface.
func (m *message) Nack() {}
