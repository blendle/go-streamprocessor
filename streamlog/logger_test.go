package streamlog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMessage(t *testing.T) {
	t.Parallel()

	m := stream.TestMessage(t, "key", "value")

	field := Message(m)

	assert.Equal(t, zap.Object("streamMessage", msg(m)), field)
}

func TestMessage_MarshalLogObject(t *testing.T) {
	t.Parallel()

	enc := zapcore.NewMapObjectEncoder()

	m := msg(stream.TestMessage(t, "key", "value"))

	err := m.MarshalLogObject(enc)
	require.NoError(t, err)

	assert.Equal(t, string(m.Key), enc.Fields["key"])
	assert.Equal(t, string(m.Value), enc.Fields["value"])
	assert.Equal(t, m.Timestamp, enc.Fields["timestamp"])
	assert.Equal(t, m.ConsumerTopic, enc.Fields["consumerTopic"])
	assert.Equal(t, m.ProducerTopic, enc.Fields["producerTopic"])
	assert.Equal(t, *m.Offset, enc.Fields["offset"])

	require.Len(t, enc.Fields["tags"], len(m.Tags))
	for k, v := range m.Tags {
		assert.Equal(t, string(v), enc.Fields["tags"].(map[string]interface{})[k].(string))
	}
}
