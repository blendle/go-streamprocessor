package streamconfig

import (
	"fmt"
	"io"
	"time"

	"github.com/blendle/go-streamprocessor/v3/stream"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/inmemconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/kafkaconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/pubsubconfig"
	"github.com/blendle/go-streamprocessor/v3/streamconfig/standardstreamconfig"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	uuid "github.com/satori/go.uuid"
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
		defaults := ConsumerDefaults

		c = &defaults
		c.Inmem = inmemconfig.ConsumerDefaults
		c.Kafka = kafkaconfig.ConsumerDefaults
		c.Pubsub = pubsubconfig.ConsumerDefaults
		c.Standardstream = standardstreamconfig.ConsumerDefaults
	}

	if p == nil {
		defaults := ProducerDefaults

		p = &defaults
		p.Inmem = inmemconfig.ProducerDefaults
		p.Kafka = kafkaconfig.ProducerDefaults
		p.Pubsub = pubsubconfig.ProducerDefaults
		p.Standardstream = standardstreamconfig.ProducerDefaults
	}

	f(c, p)
}

// ConsumerOptions is a convenience accessor to manually set consumer options.
func ConsumerOptions(fn func(c *Consumer)) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		fn(c)
	})
}

// ProducerOptions is a convenience accessor to manually set producer options.
func ProducerOptions(fn func(p *Producer)) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		fn(p)
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

// InmemListen configures the inmem consumer to continuously listen for any new
// messages in the configured store.
//
// This option has no effect when applied to a producer.
func InmemListen() Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Inmem.ConsumeOnce = false
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

// KafkaGroupIDRandom sets the group ID for the consumer to a random ID. This
// can be used to configure one-off consumers that should not share their state
// in a consumer group.
//
// This option has no effect when applied to a producer.
func KafkaGroupIDRandom() Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Kafka.GroupID = fmt.Sprintf("processor-%s", uuid.Must(uuid.NewV4()))
	})
}

// KafkaHandleTransientErrors passes _all_ errors to the errors channel,
// including the ones that are considered "transient", and the consumer or
// producer can resolve themselves eventually.
func KafkaHandleTransientErrors() Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.IgnoreErrors = []kafka.ErrorCode{}
		p.Kafka.IgnoreErrors = []kafka.ErrorCode{}
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

// KafkaMaxDeliveryRetries sets the MaxDeliveryRetries.
//
// This option has no effect when applied to a consumer.
func KafkaMaxDeliveryRetries(i int) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxDeliveryRetries = i
	})
}

// KafkaMaxInFlightRequests sets the maximum allowed in-flight requests for both
// consumers and producers.
func KafkaMaxInFlightRequests(i int) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.MaxInFlightRequests = i
		p.Kafka.MaxInFlightRequests = i
	})
}

// KafkaMaxPollInterval sets the maximum allowed poll timeout.
//
// This option has no effect when applied to a producer.
func KafkaMaxPollInterval(d time.Duration) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Kafka.MaxPollInterval = d
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

// KafkaOffsetHead sets the OffsetDefault.
//
// This option has no effect when applied to a producer.
func KafkaOffsetHead(i uint32) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		i64 := int64(i)

		c.Kafka.OffsetDefault = &i64
	})
}

// KafkaOffsetInitial sets the OffsetInitial.
//
// This option has no effect when applied to a producer.
func KafkaOffsetInitial(s kafkaconfig.Offset) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Kafka.OffsetInitial = s
	})
}

// KafkaOffsetTail sets the OffsetDefault.
//
// This option has no effect when applied to a producer.
func KafkaOffsetTail(i uint32) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		i64 := -int64(i)

		c.Kafka.OffsetDefault = &i64
	})
}

// KafkaOrderedDelivery sets `MaxInFlightRequests` to `1` for the producer, to
// guarantee ordered delivery of messages.
//
// see: https://git.io/vpgiV
// see: https://git.io/vpgDg
//
// This option has no effect when applied to a consumer.
func KafkaOrderedDelivery() Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.MaxInFlightRequests = 1
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

// KafkaRetryBackoff configures the producer to use the configured retry
// backoff before retrying a connection failure. See `KafkaMaxDeliveryRetries`
// to configure the amount of retries to execute before returning an error.
//
// This option has no effect when applied to a consumer.
func KafkaRetryBackoff(d time.Duration) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Kafka.RetryBackoff = d
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

// KafkaStatisticsInterval configures the producer or consumer to use the
// specified statistics interval.
func KafkaStatisticsInterval(d time.Duration) Option {
	return optionFunc(func(c *Consumer, p *Producer) {
		c.Kafka.StatisticsInterval = d
		p.Kafka.StatisticsInterval = d
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

// StandardstreamWriter sets the writer to use as the message stream to write
// to.
//
// This option has no effect when applied to a consumer.
func StandardstreamWriter(w io.Writer) Option {
	return optionFunc(func(_ *Consumer, p *Producer) {
		p.Standardstream.Writer = w
	})
}

// StandardstreamReader sets the reader to use as the message stream from which
// to read.
//
// This option has no effect when applied to a producer.
func StandardstreamReader(w io.ReadCloser) Option {
	return optionFunc(func(c *Consumer, _ *Producer) {
		c.Standardstream.Reader = w
	})
}
