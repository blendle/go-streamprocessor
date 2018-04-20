package stream

import "time"

// Message represents a single object received or sent to a stream, using one of
// the configured stream consumers or producers.
type Message struct {
	Value     []byte
	Key       []byte
	Timestamp time.Time
}
