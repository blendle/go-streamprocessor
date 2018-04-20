package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
)

type FakeClient struct {
}

func (fc *FakeClient) NewConsumer(options ...func(interface{})) (stream.Consumer, error) {
	return nil, nil
}

func (fc *FakeClient) NewProducer(options ...func(interface{})) (stream.Producer, error) {
	return nil, nil
}

func TestClient(t *testing.T) {
	var _ stream.Client = (*FakeClient)(nil)
}
