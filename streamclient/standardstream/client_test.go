package standardstream_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	_ "github.com/blendle/go-streamprocessor/test"
)

func TestNewClient(t *testing.T) {
	c := standardstream.NewClient()

	_, ok := c.(stream.Client)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestNewConsumerAndProducer(t *testing.T) {
	client := standardstream.NewClient()
	c, p := client.NewConsumerAndProducer()

	_, ok := c.(stream.Consumer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Consumer")
	}

	_, ok = p.(stream.Producer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, p, "stream.Producer")
	}
}
