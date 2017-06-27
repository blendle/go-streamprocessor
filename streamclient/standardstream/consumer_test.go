package standardstream_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	_ "github.com/blendle/go-streamprocessor/test"
)

func TestNewConsumer(t *testing.T) {
	client := standardstream.NewClient(&standardstream.ClientConfig{})
	c := client.NewConsumer()

	_, ok := c.(stream.Consumer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestConsumer_Messages(t *testing.T) {
	f, _ := ioutil.TempFile("", "")
	f.Write([]byte("hello world\nhello universe"))
	defer os.Remove(f.Name())

	config := &standardstream.ClientConfig{ConsumerFD: f}
	client := standardstream.NewClient(config)

	c := client.NewConsumer()

	var msg *stream.Message
	msg = <-c.Messages()

	expected := "hello world"
	actual := string(msg.Value)
	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	msg = <-c.Messages()

	expected = "hello universe"
	actual = string(msg.Value)
	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	_, ok := <-c.Messages()

	if ok != false {
		t.Errorf("Channel is not closed")
	}
}

func BenchmarkConsumer_Messages1000(b *testing.B) {
	content := `{"number":%d}` + "\n"
	tmpfile, _ := ioutil.TempFile("", "")
	tmpfile.Close()
	defer os.Remove(tmpfile.Name())

	f, _ := os.OpenFile(tmpfile.Name(), os.O_APPEND|os.O_WRONLY, 0600)
	defer f.Close()

	for n := 1; n < b.N; n++ {
		f.WriteString(fmt.Sprintf(content, n))
	}

	config := &standardstream.ClientConfig{ConsumerFD: f}
	c := standardstream.NewClient(config)

	b.ResetTimer()

	i := 0
	consumer := c.NewConsumer()
	for msg := range consumer.Messages() {
		i = i + 1
		m := bytes.Split(msg.Value, []byte(`"number":`))
		m = bytes.Split(m[1], []byte(`}`))

		expected := strconv.Itoa(i)
		actual := string(m[0])
		if actual != expected {
			b.Errorf("Unexpected return value, expected %s, got %s (message: %q)", expected, actual, msg.Value)
		}
	}
}
