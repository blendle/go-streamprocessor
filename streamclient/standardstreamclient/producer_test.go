package standardstreamclient_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"testing"
	"time"

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

	producer, err := standardstreamclient.NewProducer()
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.Equal(t, "*standardstreamclient.Producer", reflect.TypeOf(producer).String())
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t)

	options := func(c *streamconfig.Producer) {
		c.Standardstream.Writer = buffer
	}

	producer, err := standardstreamclient.NewProducer(options)
	require.NoError(t, err)
	defer func() { require.NoError(t, producer.Close()) }()

	assert.EqualValues(t, buffer, producer.Config().(streamconfig.Producer).Standardstream.Writer)
}

func TestProducer_Messages(t *testing.T) {
	t.Parallel()

	expected := "hello world\n"
	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	producer.Messages() <- stream.Message{Value: []byte(expected)}
	closer()

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, expected, string(b))
}

func TestProducer_Messages_AppendNewline(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	producer.Messages() <- stream.Message{Value: []byte("hello world")}
	closer()

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, "hello world\n", string(b))
}

func TestProducer_Messages_Ordering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- stream.Message{Value: []byte(strconv.Itoa(i))}
	}
	closer()

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)

	i := 0
	scanner := bufio.NewScanner(bytes.NewReader(b))
	for scanner.Scan() {
		assert.Equal(t, strconv.Itoa(i), scanner.Text())

		i++
	}

	assert.Equal(t, messageCount, i)
	assert.NoError(t, scanner.Err())
}

func TestProducer_Errors(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Producer) {
		c.HandleErrors = true
	}

	b := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, b, options)
	defer closer()

	err := <-producer.Errors()
	require.Error(t, err)
	assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
}

func TestProducer_Errors_Manual(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Producer) {
		c.HandleErrors = false
	}

	b := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, b, options)
	defer closer()

	select {
	case err := <-producer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(10 * time.Millisecond):
	}
}

func BenchmarkProducer_Messages(b *testing.B) {
	buffer := standardstreamclient.TestBuffer(b)
	producer, closer := standardstreamclient.TestProducer(b, buffer)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- stream.Message{Value: []byte(fmt.Sprintf(`{"number":%d}`, i))}
	}
}
