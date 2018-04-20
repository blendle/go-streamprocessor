package standardstreamclient_test

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streammsg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Consumer{}
}

func TestNewConsumer(t *testing.T) {
	t.Parallel()

	consumer, err := standardstreamclient.NewConsumer()
	require.NoError(t, err)

	assert.Equal(t, "*standardstreamclient.Consumer", reflect.TypeOf(consumer).String())
}

func TestNewConsumer_WithOptions(t *testing.T) {
	t.Parallel()

	f := standardstreamclient.TestBuffer(t)

	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = f
	}

	consumer, err := standardstreamclient.NewConsumer(options)
	require.NoError(t, err)

	assert.EqualValues(t, f, consumer.Config().Standardstream.Reader)
}

func TestConsumer_Messages(t *testing.T) {
	t.Parallel()

	buffer := standardstreamclient.TestBuffer(t, "hello world", "hello universe!")
	consumer, closer := standardstreamclient.TestConsumer(t, buffer)
	defer closer()

	msg := <-consumer.Messages()
	assert.Equal(t, "hello world", string(msg.Value))

	msg = <-consumer.Messages()
	assert.Equal(t, "hello universe!", string(msg.Value))

	_, ok := <-consumer.Messages()
	assert.False(t, ok, "consumer did not close after last message")
}

func TestConsumer_Messages_Ordering(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	buffer := standardstreamclient.TestBuffer(t)
	for i := 0; i < messageCount; i++ {
		_, err := buffer.Write([]byte(strconv.Itoa(i) + "\n"))
		require.NoError(t, err)
	}

	consumer, closer := standardstreamclient.TestConsumer(t, buffer)
	defer closer()

	i := 0
	for msg := range consumer.Messages() {
		assert.Equal(t, strconv.Itoa(i), string(msg.Value))

		i++
	}

	assert.Equal(t, messageCount, i)
}

func TestConsumer_Messages_PerMessageMemoryAllocation(t *testing.T) {
	t.Parallel()

	messageCount := 100000
	buffer := standardstreamclient.TestBuffer(t)
	for i := 0; i < messageCount; i++ {
		_, err := buffer.Write([]byte(fmt.Sprintf(`{"number":%d}`+"\n", i)))
		require.NoError(t, err)
	}

	consumer, closer := standardstreamclient.TestConsumer(t, buffer)
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

		assert.Equal(t, strconv.Itoa(i), string(m[0]))

		i++
	}
}

func TestConsumer_Messages_ScannerError(t *testing.T) {
	t.Parallel()

	if os.Getenv("BE_TESTING_FATAL") == "1" {
		// This byte array is longer than is accepted by
		// `standardstreamclient.maxCapacity`, which causes a Fatal error to occurs.
		b := make([]byte, 512*1024)

		buffer := standardstreamclient.TestBuffer(t, string(b))
		consumer, closer := standardstreamclient.TestConsumer(t, buffer)
		defer closer()

		<-consumer.Messages()

		// Give the errors channel a few milliseconds to receive the error, and
		// terminate the program.
		time.Sleep(5 * time.Millisecond)

		return
	}

	// TODO: this should probably be easier to test, but I haven't found a way yet
	//       to test Zap's `Fatal` log level without actually exiting the binary.
	cmd := exec.Command(os.Args[0], "-test.run="+t.Name())
	cmd.Env = append(os.Environ(), "BE_TESTING_FATAL=1")

	out, err := cmd.CombinedOutput()
	require.NotNil(t, err, "output received: %s", string(out))

	assert.False(t, err.(*exec.ExitError).Success())
}

func TestConsumer_Errors(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Consumer) {
		c.HandleErrors = true
	}

	b := standardstreamclient.TestBuffer(t)
	consumer, closer := standardstreamclient.TestConsumer(t, b, options)
	defer closer()

	err := <-consumer.Errors()
	require.Error(t, err)
	assert.Equal(t, "unable to manually consume errors while HandleErrors is true", err.Error())
}

func TestConsumer_Errors_Manual(t *testing.T) {
	t.Parallel()

	options := func(c *streamconfig.Consumer) {
		c.HandleErrors = false
	}

	b := standardstreamclient.TestBuffer(t)
	consumer, closer := standardstreamclient.TestConsumer(t, b, options)
	defer closer()

	select {
	case err := <-consumer.Errors():
		t.Fatalf("expected no error, got %s", err.Error())
	case <-time.After(10 * time.Millisecond):
	}
}

func TestConsumer_Ack(t *testing.T) {
	t.Parallel()

	b := standardstreamclient.TestBuffer(t)
	consumer, closer := standardstreamclient.TestConsumer(t, b)
	defer closer()

	assert.Nil(t, consumer.Ack(streammsg.Message{}))
}

func TestConsumer_Nack(t *testing.T) {
	t.Parallel()

	b := standardstreamclient.TestBuffer(t)
	consumer, closer := standardstreamclient.TestConsumer(t, b)
	defer closer()

	assert.Nil(t, consumer.Nack(streammsg.Message{}))
}

func BenchmarkConsumer_Messages(b *testing.B) {
	buffer := standardstreamclient.TestBuffer(b)
	for i := 1; i <= b.N; i++ {
		_, _ = buffer.Write([]byte(fmt.Sprintf(`{"number":%d}`+"\n", i)))
	}

	b.ResetTimer()

	consumer, closer := standardstreamclient.TestConsumer(b, buffer)
	defer closer()

	for range consumer.Messages() {
	}
}
