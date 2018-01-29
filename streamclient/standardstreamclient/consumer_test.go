package standardstreamclient_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

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

	f, _ := ioutil.TempFile("", "")
	_, err := f.WriteString("hello world\nhello universe!")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = f.Seek(0, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
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

	options := func(c *streamconfig.Consumer) {
		c.Standardstream.Reader = f
	}

	consumer, err := client.NewConsumer(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
