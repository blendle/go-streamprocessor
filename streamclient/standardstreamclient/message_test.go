package standardstreamclient

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streammsg"
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

	actual := string(msg.Value())
	expected := "testValue"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func TestMessage_SetValue(t *testing.T) {
	t.Parallel()

	str, msg := newMessage()
	msg.SetValue([]byte("testValue2"))

	actual := string(str.value)
	expected := "testValue2"

	if actual != expected {
		t.Errorf("Unexpected outcome. Expected: %v, got: %v", expected, actual)
	}
}

func newMessage() (*message, testMessage) {
	m := &message{
		value: []byte("testValue"),
	}

	return m, testMessage(m)
}
