package standardstreamclient_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io/ioutil"
	"reflect"
	"strconv"
	"testing"

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

	assert.EqualValues(t, buffer, producer.Config().Standardstream.Writer)
}

func TestNewProducer_Messages(t *testing.T) {
	t.Parallel()

	expected := "hello world\n"
	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	producer.Messages() <- producer.NewMessage([]byte(expected))
	closer()

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, expected, string(b))
}

func TestNewProducer_AppendNewline(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	producer.Messages() <- producer.NewMessage([]byte("hello world"))
	closer()

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, "hello world\n", string(b))
}

func TestNewProducer_MessageOrdering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	buffer := standardstreamclient.TestBuffer(t)
	producer, closer := standardstreamclient.TestProducer(t, buffer)

	for i := 0; i < messageCount; i++ {
		producer.Messages() <- producer.NewMessage([]byte(strconv.Itoa(i)))
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

	assert.NoError(t, scanner.Err())
}

func BenchmarkProducer_Messages(b *testing.B) {
	buffer := standardstreamclient.TestBuffer(b)
	producer, closer := standardstreamclient.TestProducer(b, buffer)
	defer closer()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		producer.Messages() <- producer.NewMessage([]byte(fmt.Sprintf(`{"number":%d}`, i)))
	}
}
