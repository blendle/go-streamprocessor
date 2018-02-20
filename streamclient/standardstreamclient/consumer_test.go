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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Consumer{}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
	require.NoError(t, err)

	consumer, err := client.NewConsumer()
	require.NoError(t, err)

	assert.Equal(t, "*standardstreamclient.Consumer", reflect.TypeOf(consumer).String())
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	f, closer := createTestFile(t, nil)
	defer closer()

	client, err := standardstreamclient.New()
	require.NoError(t, err)

	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = f
	}

	consumer, err := client.NewConsumer(options)
	require.NoError(t, err)

	assert.EqualValues(t, f, consumer.Config().Standardstream.Reader)
}

func TestNewConsumer_Messages(t *testing.T) {
	t.Parallel()

	f, closer := createTestFile(t, []byte("hello world\nhello universe!"))
	defer closer()

	consumer, closer := newConsumer(t, f)
	defer closer()

	msg := <-consumer.Messages()
	assert.Equal(t, "hello world", string(msg.Value()))

	msg = <-consumer.Messages()
	assert.Equal(t, "hello universe!", string(msg.Value()))

	_, ok := <-consumer.Messages()
	assert.False(t, ok, "consumer did not close after last message")
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
		assert.Equal(t, strconv.Itoa(i), string(msg.Value()))

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
		m := bytes.Split(msg.Value(), []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		assert.Equal(t, strconv.Itoa(i), string(m[0]))

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
