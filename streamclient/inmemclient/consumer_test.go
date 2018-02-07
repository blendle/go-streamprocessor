package inmemclient_test

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Consumer{}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	client, err := inmemclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	consumer, err := client.NewConsumer()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "*inmemclient.Consumer"
	actual := reflect.TypeOf(consumer).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()

	client, err := inmemclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Consumer) {
		c.Inmem.Store = store
	}

	consumer, err := client.NewConsumer(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := store
	actual := consumer.Config().Inmem.Store

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumer_Messages(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()
	store.Add(newKVMessage("key1", "hello world"))
	store.Add(newKVMessage("key1", "hello universe!"))

	consumer, closer := newConsumer(t, store)
	defer closer()

	msg := <-consumer.Messages()

	expected := "hello world"
	actual := string(msg.Value())

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	msg = <-consumer.Messages()

	expected = "hello universe!"
	actual = string(msg.Value())

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	_, ok := <-consumer.Messages()
	if ok {
		t.Errorf("Channel is not closed")
	}
}

func TestNewConsumer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.NewStore()

	for i := 0; i < messageCount; i++ {
		store.Add(newKVMessage(strconv.Itoa(i), "hello world"+strconv.Itoa(i)))
	}

	consumer, closer := newConsumer(t, store)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		kr, ok := msg.(streammsg.KeyReader)
		if !ok {
			t.Fatal("unable to convert message to correct interface")
		}

		if "hello world"+strconv.Itoa(i) != string(msg.Value()) {
			t.Fatalf("Expected %q to equal %d", msg.Value(), i)
		}

		if strconv.Itoa(i) != string(kr.Key()) {
			t.Fatalf("Expected %q to equal %q", kr.Key(), i)
		}

		i++
	}
}

func TestNewConsumer_PerMessageMemoryAllocation(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.NewStore()
	line := `{"number":%d}` + "\n"

	for i := 0; i < messageCount; i++ {
		store.Add(newKVMessage(strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	consumer, closer := newConsumer(t, store)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		// By making this test do some "work" during the processing of a message, we
		// trigger a potential race condition where the actual value of the message
		// is already replaced with a newer message in the channel. This is fixed in
		// this consumer's implementation, but without this test, we couldn't expose
		// the actual problem.
		m := bytes.Split(msg.Value(), []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		expected := strconv.Itoa(i)
		actual := string(m[0])
		if actual != expected {
			t.Errorf("Unexpected return value, expected %s, got %s", expected, actual)
		}

		i++
	}
}

func BenchmarkConsumer_Messages(b *testing.B) {
	store := inmemstore.NewStore()
	line := `{"number":%d}` + "\n"

	for i := 1; i <= b.N; i++ {
		store.Add(newKVMessage(strconv.Itoa(i), fmt.Sprintf(line, i)))
	}

	b.ResetTimer()

	consumer, closer := newConsumer(b, store)
	defer closer()

	for range consumer.Messages() {
	}
}

func newConsumer(tb testing.TB, s *inmemstore.Store) (stream.Consumer, func()) {
	client, err := inmemclient.New()
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Consumer) {
		c.Inmem.Store = s
	}

	consumer, err := client.NewConsumer(options)
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	fn := func() {
		err = consumer.Close()
		if err != nil {
			tb.Fatalf("Unexpected error: %v", err)
		}
	}

	return consumer, fn
}

func newKVMessage(k, v string) ([]byte, []byte, time.Time, string, int64, int32, map[string]string) {
	return []byte(k), []byte(v), time.Time{}, "", 0, 0, map[string]string{}
}
