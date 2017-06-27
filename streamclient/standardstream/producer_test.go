package standardstream_test

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	_ "github.com/blendle/go-streamprocessor/test"
)

func TestNewProducer(t *testing.T) {
	client := standardstream.NewClient(&standardstream.ClientConfig{})
	c := client.NewProducer()

	_, ok := c.(stream.Producer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestProducer_Messages(t *testing.T) {
	var b bytes.Buffer
	writer := bufio.NewWriter(&b)

	config := &standardstream.ClientConfig{ProducerFD: writer}
	client := standardstream.NewClient(config)

	c := client.NewProducer()

	msg := &stream.Message{Value: []byte("hello world")}
	c.Messages() <- msg
	c.Close()
	writer.Flush()

	expected := "hello world\n"
	actual := b.String()
	if actual != expected {
		t.Errorf("Expected %s to equal %s", actual, expected)
	}
}

func BenchmarkProducer_Messages1000(b *testing.B) {
	content := `{"number":%d}`

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	config := &standardstream.ClientConfig{ProducerFD: writer}
	client := standardstream.NewClient(config)

	producer := client.NewProducer()

	for n := 1; n < b.N; n++ {
		producer.Messages() <- &stream.Message{Value: []byte(fmt.Sprintf(content, n))}
	}

	writer.Flush()

	b.ResetTimer()

	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	i := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		i = i + 1
		m := bytes.Split(line, []byte(`{"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		expected := strconv.Itoa(i)
		actual := string(m[0])
		if actual != expected {
			b.Errorf("Unexpected return value, expected %s, got %s (message: %q)", expected, actual, line)
		}
	}
}
