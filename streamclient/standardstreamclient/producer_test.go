package standardstreamclient_test

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
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

	expected := "*standardstreamclient.Producer"
	actual := reflect.TypeOf(producer).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewProducer_WithOptions(t *testing.T) {
	t.Parallel()

	f, _ := ioutil.TempFile("", "")
	defer func() {
		err := os.Remove(f.Name())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}()

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

	expected := "hello world\n"

	producer.Messages() <- &stream.Message{Value: []byte(expected)}
	err = producer.Close()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	err = f.Flush()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	actual := b.String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
