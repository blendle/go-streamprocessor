package streamclient

import (
	"errors"
	"os"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/streamconfig"
)

// NewProducer returns a new streamclient producer, based on the context from
// which this function is called.
func NewProducer(options ...func(*streamconfig.Producer)) (stream.Producer, error) {
	switch os.Getenv("STREAMCLIENT_PRODUCER") {
	case "standardstream":
		return standardstreamclient.NewProducer(options...)
	case "inmem":
		return inmemclient.NewProducer(options...)
	case "kafka":
		return kafkaclient.NewProducer(options...)
	case "pubsub":
		return nil, errors.New("pubsub producer not implemented yet")
	}

	if os.Getenv("DRY_RUN") != "" {
		return standardstreamclient.NewProducer(options...)
	}

	return nil, errors.New("unable to determine required producer streamclient")
}
