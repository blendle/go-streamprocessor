package kafkaconfig

import (
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the Kafka client's consumer will behave.
type Consumer struct {
	// Brokers is a list of host/port pairs to use for establishing the initial
	// connection to the Kafka cluster. The client will make use of all servers
	// irrespective of which servers are specified here for bootstrapping — this
	// list only impacts the initial hosts used to discover the full set of
	// servers. Since these servers are just used for the initial connection to
	// discover the full cluster membership (which may change dynamically), this
	// list need not contain the full set of servers (you may want more than one,
	// though, in case a server is down).
	Brokers []string `kafka:"metadata.broker.list,omitempty"`

	// CommitInterval represents the frequency in milliseconds that the
	// consumer offsets are auto-committed to Kafka.
	CommitInterval time.Duration `kafka:"auto.commit.interval.ms,omitempty" split_words:"true"`

	// Debug allows tweaking of the default debug values.
	Debug Debug `kafka:"debug,omitempty"`

	// GroupID is a unique string that identifies the consumer group this consumer
	// belongs to. This property is required if the consumer uses either the group
	// management functionality by using subscribe(topic) or the Kafka-based
	// offset management strategy.
	GroupID string `kafka:"group.id,omitempty" split_words:"true"`

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
	// ignore". These errors will not be transmitted over the Consumer's errors
	// channel, as the underlying library will handle those errors gracefully.
	IgnoreErrors []kafka.ErrorCode

	// MaxInFlightRequests dictates the maximum number of in-flight requests per
	// broker connection. This is a generic property applied to all broker
	// communication, however it is primarily relevant to produce requests. In
	// particular, note that other mechanisms limit the number of outstanding
	// consumer fetch request per broker to one.
	MaxInFlightRequests int `kafka:"max.in.flight.requests.per.connection,omitempty" split_words:"true"` // nolint: lll

	// OffsetDefault sets an offset starting point from which to consume messages.
	// If this value is set to zero (0), the value is ignored, and the
	// `OffsetInitial` is used instead (see its description for more details). If
	// the value is a positive integer, the offset will be set as expected,
	// meaning that a value of `5` will set the starting offset to 5. If we
	// provide a negative integer, we parse the value as a "tail" offset, meaning
	// that an integer of `-2` will be considered as "get the second message from
	// the last known offset of the topic".
	//
	// NOTE that this value ONLY works when used in a consumer group that has no
	// offsets committed to Kafka yet. As soon as the first offset is committed
	// (per partition), this value is ignored for that consumer group, and the
	// consumer will instead continue reading from the last known offset.
	//
	// If you want to make sure that the provided offset is _always_ used as a
	// starting point, you can use this value in conjunction with
	// `streamconfig.GroupIDRandom()`.
	OffsetDefault *int64 `kafka:"-" split_words:"true"`

	// OffsetInitial dictates what to do when there is no initial offset in Kafka
	// or if the current offset does not exist any more on the server (e.g.
	// because that data has been deleted):
	//
	// * OffsetBeginning: automatically reset the offset to the earliest offset
	// * OffsetEnd: automatically reset the offset to the latest offset
	// * none: throw exception to the consumer if no previous offset is found for
	//   the consumer's group
	OffsetInitial Offset `kafka:"{topic}.auto.offset.reset,omitempty" split_words:"true"`

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

	// Topics is a list of topics to which to subscribe for this consumer.
	Topics []string `kafka:"-"`
}

// staticConsumer is a private struct, used to define default configuration
// values that can't be altered in any way. Some of these can eventually become
// public if need be, but to reduce the configuration API surface, they
// currently aren't.
type staticConsumer struct {
	// EnableEventsChannel enables the Events() channel. Messages and events will
	// be pushed on the Events() channel and the Poll() interface will be
	// disabled. This is enabled by default (and can't be disabled), since the
	// kafkaclient implementation relies on this channel being enabled.
	EnableEventsChannel bool `kafka:"go.events.channel.enable"`

	// EnableEventPartitionEOF toggles whether the "EOF" event is sent when the
	// consumer reaches the end of a partition. This is disabled, since we don't
	// use this event in the kafkaclient implementation.
	EnableEventPartitionEOF bool `kafka:"enable.partition.eof"`

	// EnableAutoCommit dictates whether to automatically and periodically commit
	// offsets in the background. Note: setting this to false does not prevent the
	// consumer from fetching previously committed start offsets. To circumvent
	// this behavior set specific start offsets per partition in the call to
	// assign().
	//
	// We enable auto-commit of messages, but _disable_ auto offset store. What
	// this means is that we use rdkafka's capability to asynchronously commit
	// offsets on a periodic basis. This improves performance by several
	// magnitudes.
	//
	// However, we still want to control in the application itself if a message
	// _should_ actually be committed or not. To do this, we disable the automated
	// offset store which means rdkafka no longer adds the offset of each received
	// message in this store. Instead, we add each message to this store
	// synchronously whenever the message is "Acked" by the application.
	//
	// Finally, on closing, or when receiving a rebalance request, we make sure to
	// call commit one final time synchronously, to drain the offset store of any
	// offsets that still need to be committed to Kafka. If any error occurs
	// during this final offset commitment, we terminate hard, making sure we have
	// to reprocess any messages that where lost in the store.
	//
	// See also: https://gist.github.com/edenhill/f617caa8ed671a0f960ead56556e0c5c
	EnableAutoCommit bool `kafka:"enable.auto.commit"`

	// EnableAutoOffsetStore dictates whether to automatically store offset of
	// last message provided to application. This is set to `false`.
	EnableAutoOffsetStore bool `kafka:"enable.auto.offset.store"`

	// EnableEventRebalance dictates whether to forward rebalancing responsibility
	// to application via the Events() channel. If set to true the app must handle
	// the AssignedPartitions and RevokedPartitions events and call Assign() and
	// Unassign() respectively. This is set to `true`, since we handle these
	// events ourselves.
	EnableEventRebalance bool `kafka:"go.application.rebalance.enable"`

	// QueuedMinMessages dictates the minimum number of messages per
	// topic+partition librdkafka tries to maintain in the local consumer queue.
	//
	// See: https://git.io/vp5eH
	QueuedMinMessages int `kafka:"queued.min.messages"`
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	CommitInterval:    5 * time.Second,
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
		kafka.ErrMaxPollExceeded,
	},
	MaxInFlightRequests: 1000000,
	OffsetInitial:       OffsetBeginning,
	SecurityProtocol:    ProtocolPlaintext,
	SessionTimeout:      30 * time.Second,
	SSL:                 SSL{},
	StatisticsInterval:  15 * time.Minute,
}

var staticConsumerDefaults = &staticConsumer{
	EnableEventsChannel:   true,
	EnableAutoCommit:      true,
	EnableAutoOffsetStore: false,
	EnableEventRebalance:  true,
	QueuedMinMessages:     500000,
}

// ConfigMap converts the current configuration into a format known to the
// rdkafka library.
func (c *Consumer) ConfigMap() (*kafka.ConfigMap, error) {
	return configMap(c, staticConsumerDefaults), c.validate()
}

func (c *Consumer) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("required config Kafka.Brokers empty")
	}

	if len(c.Topics) == 0 {
		return errors.New("required config Kafka.Topics empty")
	}

	for i := range c.Topics {
		if len(c.Topics[i]) == 0 {
			return errors.New("empty value detected in config Kafka.Topics")
		}
	}

	if len(c.GroupID) == 0 {
		return errors.New("required config Kafka.GroupID missing")
	}

	return nil
}
