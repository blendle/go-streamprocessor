package inmemstore

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streammsg"
)

// testMessage contains all the interfaces that the store has to implement.
type testMessage interface {
	streammsg.Message

	streammsg.KeyReader
	streammsg.KeyWriter
	streammsg.TimestampReader
	streammsg.TimestampWriter
	streammsg.TopicReader
	streammsg.TopicWriter
	streammsg.OffsetReader
	streammsg.PartitionReader
	streammsg.TagsReader
	streammsg.TagsWriter
	streammsg.Nacker
}

func TestMessage(t *testing.T) {
	t.Parallel()

	var _ testMessage = (*message)(nil)
}
