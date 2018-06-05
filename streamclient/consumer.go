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

// ErrUnknownConsumerClient is returned when the desired stream client cannot be
// inferred from the environment context.
var ErrUnknownConsumerClient = errors.New("unable to determine required consumer streamclient")

// NewConsumer returns a new streamclient consumer, based on the context from
// which this function is called.
func NewConsumer(options ...streamconfig.Option) (stream.Consumer, error) {
	c, err := (&streamconfig.Consumer{}).WithOptions(options...).FromEnv()
	if err != nil {
		return nil, err
	}

	switch c.Type {
	case Standardstream:
		return standardstreamclient.NewConsumer(options...)
	case Inmem:
		return inmemclient.NewConsumer(options...)
	case Kafka:
		return kafkaclient.NewConsumer(options...)
	}

	if stdinPipePresent() {
		return standardstreamclient.NewConsumer(options...)
	}

	return nil, ErrUnknownConsumerClient
}

func stdinPipePresent() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	if fi.Mode()&os.ModeNamedPipe == 0 {
		return false
	}

	return true
}
