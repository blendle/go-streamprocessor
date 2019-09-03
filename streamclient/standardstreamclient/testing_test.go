package standardstreamclient_test

import (
	"bytes"
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/v3/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestBuffer(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t)
	_, err := buffer.Write([]byte("hello world"))
	require.NoError(t, err)

	b, err := ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(b))

	buffer = standardstreamclient.TestBuffer(t, "hello", "world", "universe")
	_, err = buffer.Write([]byte("cosmos"))
	require.NoError(t, err)

	b, err = ioutil.ReadAll(buffer)
	require.NoError(t, err)
	assert.Equal(t, "hello\nworld\nuniverse\ncosmos", string(b))
}

func TestTestConsumer(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t)
	consumer, closer := standardstreamclient.TestConsumer(t, buffer)
	defer closer()

	assert.Equal(t, "*standardstreamclient.consumer", reflect.TypeOf(consumer).String())
	assert.EqualValues(t, buffer, consumer.Config().(streamconfig.Consumer).Standardstream.Reader)
}

func TestTestProducer(t *testing.T) {
	t.Parallel()

	w := &bytes.Buffer{}
	producer, closer := standardstreamclient.TestProducer(t, w)
	defer closer()

	assert.Equal(t, "*standardstreamclient.producer", reflect.TypeOf(producer).String())
	assert.EqualValues(t, w, producer.Config().(streamconfig.Producer).Standardstream.Writer)
}
