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

func TestNewConsumer(t *testing.T) {
	client := inmem.NewClient()
	c := client.NewConsumer()

	_, ok := c.(stream.Consumer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestConsumer_Messages(t *testing.T) {
	store := inmem.NewStore()
	topic := store.NewTopic("test-topic")

	topic.NewMessage([]byte("hello world"), nil)
	topic.NewMessage([]byte("hello universe"), nil)

	ct := func(c *inmem.Client) {
		c.ConsumerTopic = "test-topic"
	}

	client := inmem.NewClientWithStore(store, ct)

	c := client.NewConsumer()
	msg := <-c.Messages()

	expected := "hello world"
	actual := string(msg.Value)
	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	msg = <-c.Messages()

	expected = "hello universe"
	actual = string(msg.Value)
	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	_, ok := <-c.Messages()

	if ok {
		t.Errorf("Channel is not closed")
	}
}

func BenchmarkConsumer_Messages1000(b *testing.B) {
	content := `{"number":%d}`
	store := inmem.NewStore()
	topic := store.NewTopic("test-topic")

	for n := 1; n < b.N; n++ {
		topic.NewMessage([]byte(fmt.Sprintf(content, n)), nil)
	}

	ct := func(c *inmem.Client) {
		c.ConsumerTopic = "test-topic"
	}

	c := inmem.NewClientWithStore(store, ct)

	b.ResetTimer()

	i := 0
	consumer := c.NewConsumer()
	for msg := range consumer.Messages() {
		i = i + 1
		m := bytes.Split(msg.Value, []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		expected := strconv.Itoa(i)
		actual := string(m[0])
		if actual != expected {
			b.Errorf("Unexpected return value, expected %s, got %s (message: %q)", expected, actual, msg.Value)
		}
	}
}
