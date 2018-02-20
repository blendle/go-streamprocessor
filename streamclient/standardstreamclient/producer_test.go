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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProducer(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Producer{}
}

func TestNewProducer(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
	require.NoError(t, err)

	producer, err := client.NewProducer()
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.Equal(t, "*standardstreamclient.Producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)

	client, err := standardstreamclient.New()
	require.NoError(t, err)

	options := func(c *streamconfig.Producer) {
		c.Standardstream.Writer = f
	}

	producer, err := client.NewProducer(options)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.EqualValues(t, f, producer.Config().Standardstream.Writer)
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)
	expected := "hello world\n"

	producer, closer := newProducer(t, f)

	producer.Messages() <- producer.NewMessage([]byte(expected))
	closer()

	require.NoError(t, f.Flush())
	assert.Equal(t, expected, b.String())
}

func TestNewProducer_AppendNewline(t *testing.T) {
	t.Parallel()

	var b bytes.Buffer
	f := bufio.NewWriter(&b)
	expected := "hello world\n"

	producer, closer := newProducer(t, f)

	producer.Messages() <- producer.NewMessage([]byte("hello world"))
	closer()

	require.NoError(t, f.Flush())
	assert.Equal(t, expected, b.String())
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000

	var b bytes.Buffer
	f := bufio.NewWriter(&b)

	producer, closer := newProducer(t, f)

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- producer.NewMessage([]byte(strconv.Itoa(i)))
	}
	closer()

	require.NoError(t, f.Flush())

	i := 0
	scanner := bufio.NewScanner(bytes.NewReader(b.Bytes()))
	for scanner.Scan() {
		assert.Equal(t, strconv.Itoa(i), scanner.Text())

		i++
	}

	assert.NoError(t, scanner.Err())
}

func BenchmarkProducer_Messages(b *testing.B) {
	var bb bytes.Buffer
	f := bufio.NewWriter(&bb)

	producer, closer := newProducer(b, f)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- producer.NewMessage([]byte(fmt.Sprintf(`{"number":%d}`, i)))
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
