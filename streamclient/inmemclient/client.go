package inmemclient

import (
	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
)

// Client implements the stream.Client interface for the inmem client.
type Client struct {
	// config represents the relevant portion of the configuration passed into the
	// stream processor its initialization function.
	config inmemconfig.Client

	// rawConfig represents the as-is configuration passed into the stream
	// processor its initialization function by the user. This includes the
	// configuration of other streamclient implementations, irrelevant to the
	// current implementation.
	rawConfig streamconfig.Client
}

// New returns a new inmem client.
func New(options ...func(*streamconfig.Client)) (stream.Client, error) {
	config, err := streamconfig.NewClient(options...)
	if err != nil {
		return nil, err
	}

	client := &Client{
		config:    config.Inmem,
		rawConfig: config,
	}

	return client, nil
}

// Config returns a read-only representation of the client configuration.
func (c *Client) Config() streamconfig.Client {
	return c.rawConfig
}
