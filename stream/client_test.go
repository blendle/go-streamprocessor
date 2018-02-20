package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
)

func TestClient(t *testing.T) {
	t.Parallel()

	var _ stream.Client = (*stream.ClientMock)(nil)
}
