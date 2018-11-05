package kafkaconfig

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer is a value-object, containing all user-configurable configuration
// values that dictate how the Kafka client's producer will behave.
type Producer struct {
	// BatchMessageSize sets the maximum number of messages batched in one
	// MessageSet. The total MessageSet size is also limited by message.max.bytes.
	BatchMessageSize int `kafka:"batch.num.messages,omitempty" split_words:"true"`

	// Brokers is a list of host/port pairs to use for establishing the initial
	// connection to the Kafka cluster. The client will make use of all servers
	// irrespective of which servers are specified here for bootstrapping — this
	// list only impacts the initial hosts used to discover the full set of
	// servers. Since these servers are just used for the initial connection to
	// discover the full cluster membership (which may change dynamically), this
	// list need not contain the full set of servers (you may want more than one,
	// though, in case a server is down).
	Brokers []string `kafka:"metadata.broker.list,omitempty" split_words:"true"`

	// CompressionCodec sets the compression codec to use for compressing message
	// sets. This is the default value for all topics, may be overridden by the
	// topic configuration property compression.codec. Set tot `Snappy` by
	// default.
	CompressionCodec Compression `kafka:"compression.codec,omitempty" split_words:"true"`

	// Debug allows tweaking of the default debug values.
	Debug Debug `kafka:"debug,omitempty"`

	// HeartbeatInterval represents The expected time between heartbeats to the
	// consumer coordinator when using Kafka's group management facilities.
	// Heartbeats are used to ensure that the consumer's session stays active and
	// to facilitate rebalancing when new consumers join or leave the group. The
	// value must be set lower than `SessionTimeout`, but typically should be set
	// no higher than 1/3 of that value. It can be adjusted even lower to control
	// the expected time for normal rebalances.
	HeartbeatInterval time.Duration `kafka:"heartbeat.interval.ms,omitempty" split_words:"true"`

	// ID is an id string to pass to the server when making requests. The purpose
	// of this is to be able to track the source of requests beyond just IP/port
	// by allowing a logical application name to be included in server-side
	// request logging.
	ID string `kafka:"client.id,omitempty" envconfig:"client_id"`

	// IgnoreErrors determines what Kafka related errors are considered "safe to
	// ignore". These errors will not be transmitted over the Producer's errors
	// channel, as the underlying library will handle those errors gracefully.
	IgnoreErrors []kafka.ErrorCode

	// MaxDeliveryRetries dictates how many times to retry sending a failing
	// MessageSet. Defaults to 5 retries.
	MaxDeliveryRetries int `kafka:"message.send.max.retries" split_words:"true"`

	// MaxInFlightRequests dictates the maximum number of in-flight requests per
	// broker connection. This is a generic property applied to all broker
	// communication, however it is primarily relevant to produce requests. In
	// particular, note that other mechanisms limit the number of outstanding
	// consumer fetch request per broker to one.
	//
	// Note: having more than one in flight request may cause reordering. Use
	// `streamconfig.KafkaOrderedDelivery()` to guarantee order delivery.
	MaxInFlightRequests int `kafka:"max.in.flight.requests.per.connection,omitempty" split_words:"true"` // nolint: lll

	// MaxQueueBufferDuration is the delay to wait for messages in the producer
	// queue to accumulate before constructing message batches (MessageSets) to
	// transmit to brokers. A higher value allows larger and more effective (less
	// overhead, improved compression) batches of messages to accumulate at the
	// expense of increased message delivery latency. Defaults to 10 ms.
	MaxQueueBufferDuration time.Duration `kafka:"queue.buffering.max.ms,omitempty" split_words:"true"`

	// MaxQueueSizeKBytes is the maximum total message size sum allowed on the
	// producer queue. This property has higher priority than
	// MaxQueueSizeMessages.
	MaxQueueSizeKBytes int `kafka:"queue.buffering.max.kbytes,omitempty" envconfig:"max_queue_size_kbytes"` // nolint: lll

	// MaxQueueSizeMessages dictates the maximum number of messages allowed on the
	// producer queue.
	MaxQueueSizeMessages int `kafka:"queue.buffering.max.messages,omitempty" split_words:"true"`

	// RequiredAcks indicates how many acknowledgments the leader broker must
	// receive from ISR brokers before responding to the request:
	//
	// AckNone: Broker does not send any response/ack to client
	// AckLeader: Only the leader broker will need to ack the message,
	// AckAll: broker will block until message is committed by all in sync
	// replicas (ISRs).
	//
	// Defaults to `AckLeader`.
	RequiredAcks Ack `kafka:"{topic}.request.required.acks" split_words:"true"`

	// SecurityProtocol is the protocol used to communicate with brokers.
	SecurityProtocol Protocol `kafka:"security.protocol,omitempty" split_words:"true"`

	// SessionTimeout represents the timeout used to detect consumer failures when
	// using Kafka's group management facility. The consumer sends periodic
	// heartbeats to indicate its liveness to the broker. If no heartbeats are
	// received by the broker before the expiration of this session timeout, then
	// the broker will remove this consumer from the group and initiate a
	// rebalance. Note that the value must be in the allowable range as configured
	// in the broker configuration by `group.min.session.timeout.ms` and
	// `group.max.session.timeout.ms`.
	SessionTimeout time.Duration `kafka:"session.timeout.ms,omitempty" split_words:"true"`

	// SSL contains all configuration values for Kafka SSL connections. Defaults
	// to an empty struct, meaning no SSL configuration is required to connect to
	// the brokers.
	SSL SSL `kafka:"ssl,omitempty"`

	// StatisticsInterval is the time interval between statistics shared by Kafka
	// on how the client and cluster is performing.
	//
	// See: https://github.com/edenhill/librdkafka/wiki/Statistics
	//
	// If set to 0, no statistics will be produced. Defaults to 15 minutes.
	StatisticsInterval time.Duration `kafka:"statistics.interval.ms" split_words:"true"`

	// Topic is the topic used to deliver messages to. This value is used as a
	// default value, if the provided message does not define a topic of its own.
	Topic string `kafka:"-"`
}

// staticProducer is a private struct, used to define default configuration
// values that can't be altered in any way. Some of these can eventually become
// public if need be, but to reduce the configuration API surface, they
// currently aren't.
type staticProducer struct {
	// QueueBackpressureThreshold sets the threshold of outstanding not yet
	// transmitted requests needed to back-pressure the producer's message
	// accumulator. A lower number yields larger and more effective batches.
	// Set to 10, and non-configurable for now.
	QueueBackpressureThreshold int `kafka:"queue.buffering.backpressure.threshold"`
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	BatchMessageSize:  10000,
	CompressionCodec:  CompressionSnappy,
	Debug:             Debug{},
	HeartbeatInterval: 1 * time.Second,
	IgnoreErrors: []kafka.ErrorCode{
		// see: https://git.io/vhESH
		kafka.ErrTransport,
		kafka.ErrAllBrokersDown,

		// based on the above link, manually determined if something should be
		// considered "transient". Open question here: https://git.io/vhEdV. See
		// list of potential errors and their description here: https://git.io/vhEdi
		kafka.ErrDestroy,
		kafka.ErrFail,
		kafka.ErrResolve,
		kafka.ErrLeaderNotAvailable,
		kafka.ErrNotLeaderForPartition,
		kafka.ErrRequestTimedOut,
		kafka.ErrBrokerNotAvailable,
		kafka.ErrReplicaNotAvailable,
		kafka.ErrNetworkException,
		kafka.ErrGroupCoordinatorNotAvailable,
		kafka.ErrNotCoordinatorForGroup,
		kafka.ErrNotEnoughReplicas,
		kafka.ErrNotEnoughReplicasAfterAppend,
		kafka.ErrUnknownMemberID,
	},
	MaxDeliveryRetries:     5,
	MaxInFlightRequests:    1000000,
	MaxQueueBufferDuration: 10 * time.Millisecond,
	MaxQueueSizeKBytes:     2097151,
	MaxQueueSizeMessages:   1000000,
	RequiredAcks:           AckLeader,
	SecurityProtocol:       ProtocolPlaintext,
	SessionTimeout:         30 * time.Second,
	SSL:                    SSL{},
	StatisticsInterval:     15 * time.Minute,
}

var staticProducerDefaults = &staticProducer{
	QueueBackpressureThreshold: 10,
}

// ConfigMap converts the current configuration into a format known to the
// rdkafka library.
func (p *Producer) ConfigMap() (*kafka.ConfigMap, error) {
	return configMap(p, staticProducerDefaults), p.validate()
}

func (p *Producer) validate() error {
	if len(p.Brokers) == 0 {
		return errors.New("required config Kafka.Brokers empty")
	}

	if len(p.Topic) == 0 {
		return errors.New("required config Kafka.Topic missing")
	}

	return nil
}
