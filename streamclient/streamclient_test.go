package streamclient_test

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient"
	"github.com/blendle/go-streamprocessor/streamclient/standardstream"
	"github.com/blendle/go-streamprocessor/test"
)

func TestNewStandardStreamClient(t *testing.T) {
	c := streamclient.NewStandardStreamClient()

	_, ok := c.(stream.Client)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestNewKafkaClient(t *testing.T) {
	c := streamclient.NewKafkaClient()

	_, ok := c.(stream.Client)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Client")
	}
}

func TestNewConsumerAndProducer(t *testing.T) {
	if !*test.Kafka {
		t.Skip()
	}

	c, p := streamclient.NewConsumerAndProducer(&standardstream.ClientConfig{})

	_, ok := c.(stream.Consumer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, c, "stream.Consumer")
	}

	_, ok = p.(stream.Producer)
	if !ok {
		t.Errorf(`Expected %#v to implement "%s" interface.`, p, "stream.Producer")
	}
}

func TestNewConsumerAndProducer_KafkaConsumerAndKafkaProducer(t *testing.T) {
	if !*test.Kafka {
		t.Skip()
	}

	c, p := streamclient.NewConsumerAndProducer(&standardstream.ClientConfig{})

	expected := "*kafka.Consumer"
	actual := reflect.TypeOf(c).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	expected = "*kafka.Producer"
	actual = reflect.TypeOf(p).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumerAndProducer_KafkaConsumerAndStandardstreamProducer(t *testing.T) {
	if !*test.Kafka {
		t.Skip()
	}

	// Set the DRY_RUN environment variable to trigger standardstream as the
	// producer client
	os.Setenv("DRY_RUN", "true")
	defer os.Unsetenv("DRY_RUN")

	c, p := streamclient.NewConsumerAndProducer(&standardstream.ClientConfig{})

	expected := "*kafka.Consumer"
	actual := reflect.TypeOf(c).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	expected = "*standardstream.Producer"
	actual = reflect.TypeOf(p).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumerAndProducer_StandardstreamConsumerAndKafkaProducer(t *testing.T) {
	if !*test.Kafka {
		t.Skip()
	}

	f, _ := ioutil.TempFile("", "")
	f.Write([]byte("a"))
	defer os.Remove(f.Name())

	// Set the streamclient file descriptor to a temprary file, simulating
	// received data in the Stdin fd.
	config := &standardstream.ClientConfig{ConsumerFD: f}

	c, p := streamclient.NewConsumerAndProducer(config)

	expected := "*standardstream.Consumer"
	actual := reflect.TypeOf(c).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	expected = "*kafka.Producer"
	actual = reflect.TypeOf(p).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNewConsumerAndProducer_StandardstreamConsumerAndStandardstreamProducer(t *testing.T) {
	f, _ := ioutil.TempFile("", "")
	f.Write([]byte("a"))
	defer os.Remove(f.Name())

	// Set the streamclient file descriptor to a temprary file, simulating
	// received data in the Stdin fd.
	config := &standardstream.ClientConfig{ConsumerFD: f}

	// Set the DRY_RUN environment variable to trigger standardstream as the
	// producer client
	os.Setenv("DRY_RUN", "true")
	defer os.Unsetenv("DRY_RUN")

	c, p := streamclient.NewConsumerAndProducer(config)

	expected := "*standardstream.Consumer"
	actual := reflect.TypeOf(c).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}

	expected = "*standardstream.Producer"
	actual = reflect.TypeOf(p).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
