package streamconfig

import (
	"testing"

	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"github.com/kelseyhightower/envconfig"
	"github.com/stretchr/testify/require"
)

// TestNewConsumer returns a new consumer configuration struct, optionally with
// the default values removed.
func TestNewConsumer(tb testing.TB, defaults bool, options ...func(*Consumer)) Consumer {
	c, err := NewConsumer()
	require.NoError(tb, err)

	if !defaults {
		c.Inmem = inmemconfig.Consumer{Store: nil}
		c.Kafka = kafkaconfig.Consumer{}
		c.Pubsub = pubsubconfig.Consumer{}
		c.Standardstream = standardstreamconfig.Consumer{}
		c.Logger = nil
		c.HandleInterrupt = false
		c.HandleErrors = false
		c.Name = ""
		c.AllowEnvironmentBasedConfiguration = false
	}

	for _, option := range options {
		if option == nil {
			continue
		}

		option(&c)
	}

	err = envconfig.Process(c.Name, &c)
	require.NoError(tb, err)

	return c
}

// TestConsumerOptions returns an array of consumer options ready to be used
// during testing.
func TestConsumerOptions(tb testing.TB, options ...func(*Consumer)) []func(c *Consumer) {
	tb.Helper()

	var defaults []func(c *Consumer)

	defaults = append(defaults, func(c *Consumer) {
		c.Kafka = kafkaconfig.TestConsumer(tb)
	})

	return append(defaults, options...)
}

// TestNewProducer returns a new producer configuration struct, optionally with
// the default values removed.
func TestNewProducer(tb testing.TB, defaults bool, options ...func(*Producer)) Producer {
	p, err := NewProducer()
	require.NoError(tb, err)

	if !defaults {
		p.Inmem = inmemconfig.Producer{Store: nil}
		p.Kafka = kafkaconfig.Producer{}
		p.Pubsub = pubsubconfig.Producer{}
		p.Standardstream = standardstreamconfig.Producer{}
		p.Logger = nil
		p.HandleInterrupt = false
		p.HandleErrors = false
		p.Name = ""
		p.AllowEnvironmentBasedConfiguration = false
	}

	for _, option := range options {
		if option == nil {
			continue
		}

		option(&p)
	}

	err = envconfig.Process(p.Name, &p)
	require.NoError(tb, err)

	return p
}
