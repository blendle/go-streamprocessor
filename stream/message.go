package stream

import "time"

// Message send to or received from a stream.
type Message struct {
	Value     []byte
	Timestamp time.Time
}
