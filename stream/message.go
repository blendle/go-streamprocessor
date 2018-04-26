package stream

import (
	"errors"
	"time"
)

// Message is what is passed around by the different consumers and producers.
type Message struct {
	// Value is the actual body of the message. All stream clients use this field
	// to handle the message.
	Value []byte

	// Key is the identifier of the message. Not all stream client implementations
	// have a specific need for this field. The implementations that do use this
	// field might behave differently based on the value of this field. For
	// example, Kafka will use this value to calculate the topic partition to
	// assign this message to. If you send a message with different properties,
	// but the same key, they will always end up on the same partition.
	Key []byte

	// Timestamp can be used to order messages, if so desired. Not all stream
	// client implementations use this field, and those that do might behave
	// differently based on the value of this field.
	Timestamp time.Time

	// Tags is a set of key/value labels assigned to a message. Not all stream
	// client implementations use this field, and those that do might behave
	// differently based on the value of this field.
	Tags map[string][]byte

	// ConsumerTopic can be used by stream client consumers to expose from where
	// the message originated. Not all stream client implementations use this
	// field, and those that do might behave differently based on the value of
	// this field.
	ConsumerTopic string

	// ProducerTopic can be used by the producers to dictate on which topic the
	// message should be produced. Not all stream client implementations use this
	// field, and those that do might behave differently based on the value of
	// this field.
	ProducerTopic string

	// Offset can be used by stream client implementations to relay the position
	// in a list of messages this message has. This is a read-only value, setting
	// this value on a new message has no effect. Not all stream client
	// implementations set this field, for those that don't, this field will
	// always be `nil`.
	Offset *int64

	// opaque is an internal field used by stream client implementations to store
	// implementation specific data on the message for later use. For example, the
	// Kafka implementation uses this field to store `Ack` details on the message.
	// When you pass the message to `consumer.Ack()`, it will read this field to
	// know how to acknowledge the message.
	opaque interface{}
}

// SetMessageOpaque is a semi-private function. It is used by stream client
// implementations to set the private `opaque` field on a message. This field
// contains extra data only relevant to that client implementation. This field
// can only be set once (and is set by the stream clients), so calling this
// function outside of its intended purpose will result in an error.
func SetMessageOpaque(m *Message, o interface{}) error {
	if m.opaque != nil {
		return errors.New("opaque value can only be set once")
	}

	m.opaque = o
	return nil
}

// MessageOpqaue returns an interface object, which can be used by the same
// client implementations to handle any message logic specific to that
// implementation.
func MessageOpqaue(m *Message) interface{} {
	return m.opaque
}
