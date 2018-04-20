package inmemclient_test

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Producer{}
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	client, err := inmemclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	producer, err := client.NewProducer()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	defer func() {
		err = producer.Close()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}()

	expected := "*inmemclient.Producer"
	actual := reflect.TypeOf(producer).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()

	client, err := inmemclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Producer) {
		c.Inmem.Store = store
	}

	producer, err := client.NewProducer(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	defer func() {
		err = producer.Close()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}()

	expected := store
	actual := producer.Config().Inmem.Store

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	store := inmemstore.NewStore()
	expected := "hello world\n"

	producer, closer := newProducer(t, store)

	producer.Messages() <- producer.NewMessage([]byte(expected))
	closer()

	if len(store.Messages()) == 0 {
		t.Fatalf("expected 1 message, got %d", len(store.Messages()))
	}

	actual := string(store.Messages()[0].Value())

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	store := inmemstore.NewStore()

	producer, closer := newProducer(t, store)

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- producer.NewMessage([]byte(strconv.Itoa(i)))
	}
	closer()

	for i, msg := range store.Messages() {
		expected := strconv.Itoa(i)
		actual := string(msg.Value())

		if actual != expected {
			t.Errorf("Expected %v to equal %v", actual, expected)
		}
	}
}

func BenchmarkProducer_Messages(b *testing.B) {
	store := inmemstore.NewStore()
	producer, closer := newProducer(b, store)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- producer.NewMessage([]byte(fmt.Sprintf(`{"number":%d}`, i)))
	}
}

func newProducer(tb testing.TB, s *inmemstore.Store) (stream.Producer, func()) {
	client, err := inmemclient.New()
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Producer) {
		c.Inmem.Store = s
	}

	producer, err := client.NewProducer(options)
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	fn := func() {
		err = producer.Close()
		if err != nil {
			tb.Fatalf("Unexpected error: %v", err)
		}
	}

	return producer, fn
}
