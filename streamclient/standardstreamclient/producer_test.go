package standardstreamclient_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Producer{}
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
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

	expected := "*standardstreamclient.Producer"
	actual := reflect.TypeOf(producer).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)

	client, err := standardstreamclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Producer) {
		c.Standardstream.Writer = f
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

	expected := f
	actual := producer.Config().Standardstream.Writer

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)
	expected := "hello world\n"

	producer, closer := newProducer(t, f)

	producer.Messages() <- &stream.Message{Value: []byte(expected)}
	closer()

	err := f.Flush()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	actual := b.String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_AppendNewline(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)
	expected := "hello world\n"

	producer, closer := newProducer(t, f)

	producer.Messages() <- &stream.Message{Value: []byte("hello world")}
	closer()

	err := f.Flush()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	actual := b.String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000

	var b bytes.Buffer
	f := bufio.NewWriter(&b)

	producer, closer := newProducer(t, f)

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- &stream.Message{Value: []byte(strconv.Itoa(i))}
	}
	closer()

	err := f.Flush()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	i := 0
	scanner := bufio.NewScanner(bytes.NewReader(b.Bytes()))
	for scanner.Scan() {
		expected := strconv.Itoa(i)
		actual := scanner.Text()

		if actual != expected {
			t.Errorf("Expected %v to equal %v", actual, expected)
		}

		i++
	}

	if err = scanner.Err(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func BenchmarkProducer_Messages(b *testing.B) {
	var bb bytes.Buffer
	f := bufio.NewWriter(&bb)

	producer, closer := newProducer(b, f)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- &stream.Message{
			Value: []byte(fmt.Sprintf(`{"number":%d}`, i)),
		}
	}
}

func newProducer(tb testing.TB, w io.Writer) (stream.Producer, func()) {
	client, err := standardstreamclient.New()
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Producer) {
		c.Standardstream.Writer = w
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
