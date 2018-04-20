package streammsg

import "time"

// MessageMock is a mock implementation of all the streammsg interfaces
type MessageMock struct {
	ValueField     []byte
	KeyField       []byte
	TimestampField time.Time
	TopicField     string
	OffsetField    int64
	PartitionField int32
	TagsField      map[string][]byte
}

// Value implements the ValueReader interface for MessageMock.
func (m MessageMock) Value() []byte {
	return m.ValueField
}

// SetValue implements the ValueWriter interface for MessageMock.
func (m *MessageMock) SetValue(v []byte) {
	m.ValueField = v
}

// Key implements the KeyReader interface for MessageMock.
func (m MessageMock) Key() []byte {
	return m.KeyField
}

// SetKey implements the KeyWriter interface for MessageMock.
func (m *MessageMock) SetKey(k []byte) {
	m.KeyField = k
}

// Timestamp implements the TimestampReader interface for MessageMock.
func (m MessageMock) Timestamp() time.Time {
	return m.TimestampField
}

// SetTimestamp implements the TimestampWriter interface for MessageMock.
func (m *MessageMock) SetTimestamp(t time.Time) {
	m.TimestampField = t
}

// Topic implements the TopicReader interface for MessageMock.
func (m MessageMock) Topic() string {
	return m.TopicField
}

// SetTopic implements the TopicWriter interface for MessageMock.
func (m *MessageMock) SetTopic(t string) {
	m.TopicField = t
}

// Offset implements the OffsetReader interface for MessageMock.
func (m MessageMock) Offset() int64 {
	return m.OffsetField
}

// Partition implements the PartitionReader interface for MessageMock.
func (m MessageMock) Partition() int32 {
	return m.PartitionField
}

// Tags implements the TagsReader interface for MessageMock.
func (m MessageMock) Tags() map[string][]byte {
	return m.TagsField
}

// Tag implements the TagsReader interface for MessageMock.
func (m MessageMock) Tag(k string) []byte {
	return m.TagsField[k]
}

// SetTags implements the TagsWriter interface for MessageMock.
func (m *MessageMock) SetTags(t map[string][]byte) {
	m.TagsField = t
}

// SetTag implements the TagsWriter interface for MessageMock.
func (m *MessageMock) SetTag(k string, v []byte) {
	m.TagsField[k] = v
}

// RemoveTag implements the TagsWriter interface for MessageMock.
func (m *MessageMock) RemoveTag(k string) {
	delete(m.TagsField, k)
}

// Ack implements the Acker interface for MessageMock.
func (m *MessageMock) Ack() error {
	return nil
}

// Nack implements the Nacker interface for MessageMock.
func (m *MessageMock) Nack() error {
	return nil
}
