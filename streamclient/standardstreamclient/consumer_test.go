package standardstreamclient_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Consumer{}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	consumer, err := client.NewConsumer()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "*standardstreamclient.Consumer"
	actual := reflect.TypeOf(consumer).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	f, closer := createTestFile(t, nil)
	defer closer()

	client, err := standardstreamclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = f
	}

	consumer, err := client.NewConsumer(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := f
	actual := consumer.Config().Standardstream.Reader

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumer_Messages(t *testing.T) {
	t.Parallel()

	f, closer := createTestFile(t, []byte("hello world\nhello universe!"))
	defer closer()

	consumer, closer := newConsumer(t, f)
	defer closer()

	msg := <-consumer.Messages()

	expected := "hello world"
	actual := string(msg.Value)

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	msg = <-consumer.Messages()

	expected = "hello universe!"
	actual = string(msg.Value)

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
	fileContents := []byte("")

	for i := 0; i < messageCount; i++ {
		fileContents = append(fileContents, []byte(strconv.Itoa(i)+"\n")...)
	}

	f, closer := createTestFile(t, fileContents)
	defer closer()

	consumer, closer := newConsumer(t, f)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		if strconv.Itoa(i) != string(msg.Value) {
			t.Fatalf("Expected %q to equal %d", msg.Value, i)
		}

		i++
	}
}

func TestNewConsumer_PerMessageMemoryAllocation(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	fileContents := []byte("")
	line := `{"number":%d}` + "\n"

	for i := 0; i < messageCount; i++ {
		fileContents = append(fileContents, []byte(fmt.Sprintf(line, i))...)
	}

	f, closer := createTestFile(t, fileContents)
	defer closer()

	consumer, closer := newConsumer(t, f)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		// By making this test do some "work" during the processing of a message, we
		// trigger a potential race condition where the actual value of the message
		// is already replaced with a newer message in the channel. This is fixed in
		// this consumer's implementation, but without this test, we couldn't expose
		// the actual problem.
		m := bytes.Split(msg.Value, []byte(`"number":`))
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
	fileContents := []byte("")
	line := `{"number":%d}` + "\n"

	for i := 1; i <= b.N; i++ {
		fileContents = append(fileContents, []byte(fmt.Sprintf(line, i))...)
	}

	f, closer := createTestFile(b, fileContents)
	defer closer()

	b.ResetTimer()

	consumer, closer := newConsumer(b, f)
	defer closer()

	for range consumer.Messages() {
	}
}

func newConsumer(tb testing.TB, r io.ReadCloser) (stream.Consumer, func()) {
	client, err := standardstreamclient.New()
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = r
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

func createTestFile(tb testing.TB, b []byte) (*os.File, func()) {
	f, _ := ioutil.TempFile("", "")
	_, err := f.Write(b)
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		tb.Fatalf("Unexpected error: %v", err)
	}

	fn := func() {
		err := os.Remove(f.Name())
		if err != nil {
			tb.Fatalf("Unexpected error: %v", err)
		}
	}

	return f, fn
}
