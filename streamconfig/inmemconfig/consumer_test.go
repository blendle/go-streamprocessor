package inmemconfig_test

import (
	"reflect"
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamutils/inmemstore"
	"go.uber.org/zap"
)

func TestConsumer(t *testing.T) {
	t.Parallel()

	_ = inmemconfig.Consumer{
		Logger: zap.NewNop(),
		Store:  inmemstore.New(),
	}
}

func TestConsumerDefaults(t *testing.T) {
	t.Parallel()

	logger := zap.NewExample()
	store := inmemstore.New()

	cc := inmemconfig.Client{Logger: logger, Store: store}
	config := inmemconfig.ConsumerDefaults(cc)

	expected1 := logger
	actual1 := config.Logger

	if !reflect.DeepEqual(expected1, actual1) {
		t.Errorf("Expected %v to equal %v", actual1, expected1)
	}

	expected2 := store
	actual2 := config.Store

	if !reflect.DeepEqual(expected2, actual2) {
		t.Errorf("Expected %v to equal %v", actual2, expected2)
	}
}
