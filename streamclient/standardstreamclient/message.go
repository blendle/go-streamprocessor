package standardstreamclient

import "github.com/blendle/go-streamprocessor/streammsg"

// Message represents the interface as exposed by the standard stream client.
//
// It equals the default streammsg.Message interface, and only exposes the bare
// minimum data of a message.
type Message interface {
	streammsg.Message
}

type message struct {
	value []byte
}

// Value returns the value of the message.
func (m *message) Value() []byte {
	return m.value
}

// SetValue sets the value of the message.
func (m *message) SetValue(v []byte) {
	m.value = v
}

// Ack is a no-op implementation of the required interface.
func (m *message) Ack() {}
