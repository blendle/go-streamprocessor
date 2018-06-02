package streamlog

import (
	"github.com/blendle/go-streamprocessor/stream"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// msg is an internal representation of a `stream.Message`, used to support
// object marshalling to `zap.Field`..
type msg stream.Message

// tags is an internal representation of a set of `stream.Message` tags, used to
// support object marshalling.
type tags map[string][]byte

// Message returns a `zap.Field` object. It is a convenience method that allows
// you to easily add message contents to any log entries you create using Zap:
//
//     logger.Error(":boom:", streamlog.Message(msg))
//
// For simplicity and consistency, the field key cannot be changed. It will
// always be set to `streamMessage`. If you want to change this field, you can
// implement your own marshaller.
func Message(m stream.Message) zap.Field {
	return zap.Object("streamMessage", msg(m))
}

// MarshalLogObject implements Zap's `zapcore.ObjectMarshaler` interface. This
// implementation is not put on the original `stream.Message` object, to keep
// the API surface area of that object as small as possible.
func (m msg) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddByteString("key", m.Key)
	enc.AddByteString("value", m.Value)
	enc.AddTime("timestamp", m.Timestamp)
	enc.AddString("consumerTopic", m.ConsumerTopic)
	enc.AddString("producerTopic", m.ProducerTopic)

	if m.Offset != nil {
		enc.AddInt64("offset", *m.Offset)
	}

	if err := enc.AddObject("tags", tags(m.Tags)); err != nil {
		return err
	}

	return nil
}

// MarshalLogObject adds support for marshalling a set of tags to their proper
// Zap log message structure.
func (t tags) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for k, v := range t {
		enc.AddByteString(k, v)
	}

	return nil
}
