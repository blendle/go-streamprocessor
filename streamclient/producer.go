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

// ErrUnknownProducerClient is returned when the desired stream client cannot be
// inferred from the environment context.
var ErrUnknownProducerClient = errors.New("unable to determine required producer streamclient")

// NewProducer returns a new streamclient producer, based on the context from
// which this function is called.
func NewProducer(options ...streamconfig.Option) (stream.Producer, error) {
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

	if os.Getenv("DRY_RUN") != "" {
		return standardstreamclient.NewProducer(options...)
	}

	return nil, errors.New("unable to determine required producer streamclient")
}
