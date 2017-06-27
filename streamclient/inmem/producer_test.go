package inmem_test

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmem"
	_ "github.com/blendle/go-streamprocessor/test"
)

func TestNewProducer(t *testing.T) {
	client := inmem.NewClient()
	c := client.NewProducer()

	_, ok := c.(stream.Producer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestProducer_Messages(t *testing.T) {
	store := inmem.NewStore()
	topic := store.NewTopic("test-topic")

	pt := func(c *inmem.Client) {
		c.ProducerTopic = "test-topic"
	}

	client := inmem.NewClientWithStore(store, pt)
	c := client.NewProducer()

	msg := &stream.Message{Value: []byte("hello world")}
	c.Messages() <- msg
	c.Close()

	expected := "hello world"
	actual := string(topic.Messages()[0])
	if actual != expected {
		t.Errorf("Expected %s to equal %s", actual, expected)
	}
}

func BenchmarkProducer_Messages1000(b *testing.B) {
	content := `{"number":%d}`

	store := inmem.NewStore()
	topic := store.NewTopic("test-topic")
	client := inmem.NewClientWithStore(store)
	producer := client.NewProducer()

	for n := 1; n < b.N; n++ {
		producer.Messages() <- &stream.Message{Value: []byte(fmt.Sprintf(content, n))}
	}

	b.ResetTimer()

	for i, msg := range topic.Messages() {
		i = i + 1
		m := bytes.Split(msg, []byte(`{"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		expected := strconv.Itoa(i)
		actual := string(m[0])
		if actual != expected {
			b.Errorf("Unexpected return value, expected %s, got %s (message: %q)", expected, actual, msg)
		}
	}
}
