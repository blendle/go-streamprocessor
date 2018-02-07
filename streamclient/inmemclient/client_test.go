package inmemclient_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"go.uber.org/zap"
)

func TestClient(t *testing.T) {
	t.Parallel()

	_ = inmemclient.Client{}
}

func TestNew(t *testing.T) {
	t.Parallel()

	client, err := inmemclient.New()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := "*inmemclient.Client"
	actual := reflect.TypeOf(client).String()

	if actual != expected {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}

func TestNew_WithOptions(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()

	options := func(c *streamconfig.Client) {
		c.Inmem.Logger = logger
	}

	client, err := inmemclient.New(options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expected := logger
	actual := client.Config().Inmem.Logger

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("Expected %v to equal %v", actual, expected)
	}
}
