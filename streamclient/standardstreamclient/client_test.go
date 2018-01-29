package standardstreamclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = standardstreamclient.Client{}
}

func TestNew(t *testing.T) {
	t.Parallel()

	client, err := standardstreamclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "*standardstreamclient.Client"
	actual := reflect.TypeOf(client).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	options := func(c *streamconfig.Client) {
		c.Standardstream.Logger = logger
	}

	client, err := standardstreamclient.New(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := logger
	actual := client.Config().Standardstream.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
