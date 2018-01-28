package streamconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig"
)

func TestNewClient(t *testing.T) {
	config, err := streamconfig.NewClient()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"*streamconfig.Client", config},
		{"*inmemconfig.Client", config.Inmem},
		{"*kafkaconfig.Client", config.Kafka},
		{"*pubsubconfig.Client", config.Pubsub},
		{"*standardstreamconfig.Client", config.Standardstream},
	}

	for _, tt := range tests {
		expected := tt.expected
		actual := reflect.TypeOf(tt.config).String()

		if actual != expected {
			t.Errorf("Expected %v to equal %v", actual, expected)
		}
	}
}

func TestNewConsumer(t *testing.T) {
	config, err := streamconfig.NewConsumer()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"*streamconfig.Consumer", config},
		{"*inmemconfig.Consumer", config.Inmem},
		{"*kafkaconfig.Consumer", config.Kafka},
		{"*pubsubconfig.Consumer", config.Pubsub},
		{"*standardstreamconfig.Consumer", config.Standardstream},
	}

	for _, tt := range tests {
		expected := tt.expected
		actual := reflect.TypeOf(tt.config).String()

		if actual != expected {
			t.Errorf("Expected %v to equal %v", actual, expected)
		}
	}
}

func TestNewProducer(t *testing.T) {
	config, err := streamconfig.NewProducer()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	tests := []struct {
		expected string
		config   interface{}
	}{
		{"*streamconfig.Producer", config},
		{"*inmemconfig.Producer", config.Inmem},
		{"*kafkaconfig.Producer", config.Kafka},
		{"*pubsubconfig.Producer", config.Pubsub},
		{"*standardstreamconfig.Producer", config.Standardstream},
	}

	for _, tt := range tests {
		expected := tt.expected
		actual := reflect.TypeOf(tt.config).String()

		if actual != expected {
			t.Errorf("Expected %v to equal %v", actual, expected)
		}
	}
}
