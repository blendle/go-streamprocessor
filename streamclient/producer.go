package streamclient

import (
	"errors"
	"os"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamclient/inmemclient"
	"github.com/blendle/go-streamprocessor/v3/streamclient/kafkaclient"
	"github.com/blendle/go-streamprocessor/v3/streamclient/standardstreamclient"
	"github.com/blendle/go-streamprocessor/v3/streamconfig"
)

// ErrUnknownProducerClient is returned when the desired stream client cannot be
// inferred from the environment context.
var ErrUnknownProducerClient = errors.New("unable to determine required producer streamclient")

// NewProducer returns a new streamclient producer, based on the context from
// which this function is called.
func NewProducer(options ...streamconfig.Option) (stream.Producer, error) {
	if _, ok := os.LookupEnv("DRY_RUN"); ok {
		return standardstreamclient.NewProducer(options...)
	}

	c, err := (&streamconfig.Producer{}).WithOptions(options...).FromEnv()
	if err != nil {
		return nil, err
	}

	switch c.Type {
	case Standardstream:
		return standardstreamclient.NewProducer(options...)
	case Inmem:
		return inmemclient.NewProducer(options...)
	case Kafka:
		return kafkaclient.NewProducer(options...)
	}

	return nil, ErrUnknownProducerClient
}
