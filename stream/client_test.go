package stream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
)

type FakeClient struct {
}

func (fc *FakeClient) NewConsumer(options ...func(*streamconfig.Consumer)) (stream.Consumer, error) {
	return nil, nil
}

func (fc *FakeClient) NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	return nil, nil
}

func (fc *FakeClient) Config() streamconfig.Client {
	return streamconfig.Client{}
}

func TestClient(t *testing.T) {
	var _ stream.Client = (*FakeClient)(nil)
}
