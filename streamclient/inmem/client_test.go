package inmem_test

import (
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmem"
	_ "github.com/blendle/go-streamprocessor/test"
)

func TestNewClient(t *testing.T) {
	c := inmem.NewClient()

	_, ok := c.(stream.Client)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestNewConsumerAndProducer(t *testing.T) {
	client := inmem.NewClient()
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
