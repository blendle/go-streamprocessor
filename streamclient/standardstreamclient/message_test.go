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

	_, _ = newMessage(t)
}

func TestMessage_Value(t *testing.T) {
	t.Parallel()

	_, msg := newMessage(t)
	assert.Equal(t, "testValue", string(msg.Value()))
}

func TestMessage_SetValue(t *testing.T) {
	t.Parallel()

	str, msg := newMessage(t)
	msg.SetValue([]byte("testValue2"))

	assert.Equal(t, "testValue2", string(str.value))
}

func newMessage(tb testing.TB) (*message, testMessage) {
	tb.Helper()

	m := &message{
		value: []byte("testValue"),
	}

	return m, testMessage(m)
}
