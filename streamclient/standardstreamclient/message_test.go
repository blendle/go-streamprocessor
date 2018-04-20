package standardstreamclient

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
)

type testMessage interface {
	streammsg.Message
}

func TestMessage(t *testing.T) {
	t.Parallel()

	_, _ = newMessage()
}

func TestMessage_Value(t *testing.T) {
	t.Parallel()

	_, msg := newMessage()
	assert.Equal(t, "testValue", string(msg.Value()))
}

func TestMessage_SetValue(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetValue([]byte("testValue2"))

	assert.Equal(t, "testValue2", string(str.value))
}

func newMessage() (*message, testMessage) {
	m := &message{
		value: []byte("testValue"),
	}

	return m, testMessage(m)
}
