package streammsg

import "time"

// Message represents the minimum interface a stream message has to implement to
// be able to be consumed and produced by a stream client. Stream clients are
// free to add additional interfaces on top of this one.
type Message interface {
	ValueReader
	ValueWriter
	Acker
}

// ValueReader provides access to the value of a message.
type ValueReader interface {
	Value() []byte
}

// ValueWriter allows for setting the value of a message.
type ValueWriter interface {
	SetValue([]byte)
}

// TimestampReader provides access to message timestamps.
type TimestampReader interface {
	Timestamp() time.Time
}

// TimestampWriter allows for setting the timestamp of a message.
type TimestampWriter interface {
	SetTimestamp(time.Time)
}

// KeyReader provides access to the key of a message.
type KeyReader interface {
	Key() []byte
}

// KeyWriter allows for setting the key of a message.
type KeyWriter interface {
	SetKey([]byte)
}

// TopicReader provides access to the topic of a message.
type TopicReader interface {
	Topic() string
}

// TopicWriter allows for setting the topic of a message.
type TopicWriter interface {
	SetTopic(string)
}

// OffsetReader provides access to the offset of a message.
type OffsetReader interface {
	Offset() int64
}

// PartitionReader provides access to the partition of a message.
type PartitionReader interface {
	Partition() int32
}

// TagsReader provides access to key/value tags of a message.
type TagsReader interface {
	Tags() map[string][]byte
	Tag(string) []byte
}

// TagsWriter allows for setting key/value tags of a message.
type TagsWriter interface {
	SetTags(map[string][]byte)
	SetTag(string, []byte)
	RemoveTag(string)
}

// Acker interface allows messages to be acknowledged. Each stream client has
// its own implementation to determine what it means to ack a message.
type Acker interface {
	Ack()
}

// Nacker interface allows messages to be "not acknowledged". Each stream client
// has its own implementation to determine what it means to nack a message.
type Nacker interface {
	Nack()
}
