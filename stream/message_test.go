package stream_test

import (
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
)

func TestMessage(t *testing.T) {
	_ = stream.Message{
		Value:     []byte("testValue"),
		Key:       []byte("testKey"),
		Timestamp: time.Now(),
	}
}
