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

// NewConsumer returns a new streamclient consumer, based on the context from
// which this function is called.
func NewConsumer(options ...streamconfig.Option) (stream.Consumer, error) {
	switch os.Getenv("STREAMCLIENT_CONSUMER") {
	case "standardstream":
		return standardstreamclient.NewConsumer(options...)
	case "inmem":
		return inmemclient.NewConsumer(options...)
	case "kafka":
		return kafkaclient.NewConsumer(options...)
	case "pubsub":
		return nil, errors.New("pubsub consumer not implemented yet")
	}

	if stdinPipePresent() {
		return standardstreamclient.NewConsumer(options...)
	}

	return nil, errors.New("unable to determine required consumer streamclient")
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
