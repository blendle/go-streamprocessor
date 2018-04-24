package streamconfig

import (
	"time"

	"github.com/blendle/go-streamprocessor/stream"
	"github.com/blendle/go-streamprocessor/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/streamconfig/standardstreamconfig"
	"go.uber.org/zap"
)

// An Option configures a Consumer and/or Producer.
type Option interface {
	apply(*Consumer, *Producer)
}

// optionFunc wraps a func so it satisfies the Option interface.
type optionFunc func(*Consumer, *Producer)

func (f optionFunc) apply(c *Consumer, p *Producer) {
	if c == nil {
		c = &ConsumerDefaults
		c.Inmem = inmemconfig.ConsumerDefaults
		c.Kafka = kafkaconfig.ConsumerDefaults
		c.Pubsub = pubsubconfig.ConsumerDefaults
		c.Standardstream = standardstreamconfig.ConsumerDefaults
	}

	if p == nil {
		p = &ProducerDefaults
		p.Inmem = inmemconfig.ProducerDefaults
		p.Kafka = kafkaconfig.ProducerDefaults
		p.Pubsub = pubsubconfig.ProducerDefaults
		p.Standardstream = standardstreamconfig.ProducerDefaults
	}

	f(c, p)
}

// ConsumerOptions is a convenience accessor to manually set consumer options.
func ConsumerOptions(fn func(c *Consumer)) Option {
	c := &ConsumerDefaults
	c.Inmem = inmemconfig.ConsumerDefaults
	c.Kafka = kafkaconfig.ConsumerDefaults
	c.Pubsub = pubsubconfig.ConsumerDefaults
	c.Standardstream = standardstreamconfig.ConsumerDefaults

	fn(c)

	return optionFunc(func(c1 *Consumer, _ *Producer) {
		*c1 = *c
	})
}

// ProducerOptions is a convenience accessor to manually set producer options.
func ProducerOptions(fn func(p *Producer)) Option {
	p := &ProducerDefaults
	p.Inmem = inmemconfig.ProducerDefaults
	p.Kafka = kafkaconfig.ProducerDefaults
	p.Pubsub = pubsubconfig.ProducerDefaults
	p.Standardstream = standardstreamconfig.ProducerDefaults

	fn(p)

	return optionFunc(func(_ *Consumer, p1 *Producer) {
		*p1 = *p
	})
}

// DisableEnvironmentConfig prevents the consumer or producer to be configured
// via environment variables, instead of the default configuration to allow
// environment variable-based configurations.
func DisableEnvironmentConfig() Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.AllowEnvironmentBasedConfiguration = false
		p.AllowEnvironmentBasedConfiguration = false
	})
}

// ManualErrorHandling prevents the consumer or producer to automatically
// handle stream errors. When this option is passed, the application itself
// needs to listen to, and act on the `Errors()` channel.
func ManualErrorHandling() Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.HandleErrors = false
		p.HandleErrors = false
	})
}

// ManualInterruptHandling prevents the consumer or producer to automatically
// handle interrupt signals. When this option is passed, the application itself
// needs to handle Unix interrupt signals to properly close the consumer or
// producer when required.
func ManualInterruptHandling() Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.HandleInterrupt = false
		p.HandleInterrupt = false
	})
}

// Logger sets the logger for the consumer or producer.
func Logger(l *zap.Logger) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Logger = l
		p.Logger = l
	})
}

// Name sets the name for the consumer or producer.
func Name(s string) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Name = s
		p.Name = s
	})
}

// InmemStore adds a store to the inmem consumer and producer.
func InmemStore(s stream.Store) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Inmem.Store = s
		p.Inmem.Store = s
	})
}

// KafkaBroker adds a broker to the list of configured Kafka brokers.
func KafkaBroker(s string) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.Brokers = append(c.Kafka.Brokers, s)
		p.Kafka.Brokers = append(p.Kafka.Brokers, s)
	})
}

// KafkaCommitInterval sets the consumer's CommitInterval.
//
// This option has no effect when applied to a producer.
func KafkaCommitInterval(d time.Duration) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Kafka.CommitInterval = d
	})
}

// KafkaCompressionCodec sets the compression codec for the produced messages.
//
// // This option has no effect when applied to a consumer.
func KafkaCompressionCodec(s kafkaconfig.Compression) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.CompressionCodec = s
	})
}

// KafkaDebug enabled debugging for Kafka.
func KafkaDebug() Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.Debug.All = true
		p.Kafka.Debug.All = true
	})
}

// KafkaGroupID sets the group ID for the consumer.
//
// This option has no effect when applied to a producer.
func KafkaGroupID(s string) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Kafka.GroupID = s
	})
}

// KafkaHeartbeatInterval sets the consumer or producer HeartbeatInterval.
func KafkaHeartbeatInterval(d time.Duration) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.HeartbeatInterval = d
		p.Kafka.HeartbeatInterval = d
	})
}

// KafkaID sets the consumer or producer ID.
func KafkaID(s string) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.ID = s
		p.Kafka.ID = s
	})
}

// KafkaInitialOffset sets the InitialOffset.
//
// This option has no effect when applied to a producer.
func KafkaInitialOffset(s kafkaconfig.Offset) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.InitialOffset = s
	})
}

// KafkaMaxDeliveryRetries sets the MaxDeliveryRetries.
//
// This option has no effect when applied to a consumer.
func KafkaMaxDeliveryRetries(i int) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxDeliveryRetries = i
	})
}

// KafkaMaxQueueBufferDuration sets the MaxQueueBufferDuration.
//
// This option has no effect when applied to a consumer.
func KafkaMaxQueueBufferDuration(d time.Duration) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxQueueBufferDuration = d
	})
}

// KafkaMaxQueueSizeKBytes sets the MaxQueueSizeKBytes.
//
// This option has no effect when applied to a consumer.
func KafkaMaxQueueSizeKBytes(i int) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxQueueSizeKBytes = i
	})
}

// KafkaMaxQueueSizeMessages sets the MaxQueueSizeMessages.
//
// This option has no effect when applied to a consumer.
func KafkaMaxQueueSizeMessages(i int) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxQueueSizeMessages = i
	})
}

// KafkaRequireNoAck configures the producer not to wait for any broker acks.
//
// This option has no effect when applied to a consumer.
func KafkaRequireNoAck() Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.RequiredAcks = kafkaconfig.AckNone
	})
}

// KafkaRequireLeaderAck configures the producer wait for a single ack by the
// Kafka cluster leader broker.
//
// This option has no effect when applied to a consumer.
func KafkaRequireLeaderAck() Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.RequiredAcks = kafkaconfig.AckLeader
	})
}

// KafkaRequireAllAck configures the producer wait for a acks from all brokers
// available in the Kafka cluster.
//
// This option has no effect when applied to a consumer.
func KafkaRequireAllAck() Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.RequiredAcks = kafkaconfig.AckAll
	})
}

// KafkaSecurityProtocol configures the producer or consumer to use the
// specified security protocol.
func KafkaSecurityProtocol(s kafkaconfig.Protocol) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.SecurityProtocol = s
		p.Kafka.SecurityProtocol = s
	})
}

// KafkaSessionTimeout configures the producer or consumer to use the
// specified session timeout.
func KafkaSessionTimeout(d time.Duration) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.SessionTimeout = d
		p.Kafka.SessionTimeout = d
	})
}

// KafkaSSL configures the producer or consumer to use the specified SSL config.
func KafkaSSL(capath, certpath, crlpath, keypassword, keypath, keystorepassword, keystorepath string) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.SSL.CAPath = capath
		c.Kafka.SSL.CertPath = certpath
		c.Kafka.SSL.CRLPath = crlpath
		c.Kafka.SSL.KeyPassword = keypassword
		c.Kafka.SSL.KeyPath = keypath
		c.Kafka.SSL.KeystorePassword = keystorepassword
		c.Kafka.SSL.KeystorePath = keystorepath

		p.Kafka.SSL.CAPath = capath
		p.Kafka.SSL.CertPath = certpath
		p.Kafka.SSL.CRLPath = crlpath
		p.Kafka.SSL.KeyPassword = keypassword
		p.Kafka.SSL.KeyPath = keypath
		p.Kafka.SSL.KeystorePassword = keystorepassword
		p.Kafka.SSL.KeystorePath = keystorepath
	})
}

// KafkaTopic configures the producer or consumer to use the specified topic. In
// case of the consumer, this option can be used multiple times to consume from
// more than one topic. In case of the producer, the last usage of this option
// will set the final topic to produce to.
func KafkaTopic(s string) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.Topics = append(c.Kafka.Topics, s)
		p.Kafka.Topic = s
	})
}
