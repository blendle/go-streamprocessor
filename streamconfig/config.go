package streamconfig

import (
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
)

// Client contains the configuration for all the different clients that
// implement the stream.Client interface. When a client is instantiated, these
// options can be passed into the new client to determine its behavior. If the
// client only has to support a single implementation of the interface, then all
// other configuration values can be ignored.
type Client struct {
	Inmem          inmemconfig.Client
	Kafka          kafkaconfig.Client
	Pubsub         pubsubconfig.Client
	Standardstream standardstreamconfig.Client
}

// Consumer works the same as `streamconfig.Client`, except that it contains the
// configuration for all the consumers instantiated from the already
// instantiated client. Each client supports multiple consumers, and each
// consumer can have its own behavior, dictated by the provided
// `streamconfig.Consumer`.
type Consumer struct {
	Inmem          inmemconfig.Consumer
	Kafka          kafkaconfig.Consumer
	Pubsub         pubsubconfig.Consumer
	Standardstream standardstreamconfig.Consumer
}

// Producer works the same as `streamconfig.Consumer`, except that it dictates
// the behaviour of a producer, instead of a consumer.
type Producer struct {
	Inmem          inmemconfig.Producer
	Kafka          kafkaconfig.Producer
	Pubsub         pubsubconfig.Producer
	Standardstream standardstreamconfig.Producer
}
