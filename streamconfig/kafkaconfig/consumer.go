package kafkaconfig

import (
	"time"

	"go.uber.org/zap"
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
	Brokers []string

	// CommitInterval represents the frequency in milliseconds that the
	// consumer offsets are auto-committed to Kafka.
	CommitInterval time.Duration

	// Debug allows tweaking of the default debug values.
	Debug Debug

	// GroupID is a unique string that identifies the consumer group this consumer
	// belongs to. This property is required if the consumer uses either the group
	// management functionality by using subscribe(topic) or the Kafka-based
	// offset management strategy.
	GroupID string

	// HeartbeatInterval represents The expected time between heartbeats to the
	// consumer coordinator when using Kafka's group management facilities.
	// Heartbeats are used to ensure that the consumer's session stays active and
	// to facilitate rebalancing when new consumers join or leave the group. The
	// value must be set lower than `SessionTimeout`, but typically should be set
	// no higher than 1/3 of that value. It can be adjusted even lower to control
	// the expected time for normal rebalances.
	HeartbeatInterval time.Duration

	// ID is an id string to pass to the server when making requests. The purpose
	// of this is to be able to track the source of requests beyond just IP/port
	// by allowing a logical application name to be included in server-side
	// request logging.
	ID string

	// InitialOffset dictates what to do when there is no initial offset in Kafka
	// or if the current offset does not exist any more on the server (e.g.
	// because that data has been deleted):
	//
	// * OffsetBeginning: automatically reset the offset to the earliest offset
	// * OffsetEnd: automatically reset the offset to the latest offset
	// * none: throw exception to the consumer if no previous offset is found for
	//   the consumer's group
	InitialOffset Offset

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger

	// SessionTimeout represents the timeout used to detect consumer failures when
	// using Kafka's group management facility. The consumer sends periodic
	// heartbeats to indicate its liveness to the broker. If no heartbeats are
	// received by the broker before the expiration of this session timeout, then
	// the broker will remove this consumer from the group and initiate a
	// rebalance. Note that the value must be in the allowable range as configured
	// in the broker configuration by `group.min.session.timeout.ms` and
	// `group.max.session.timeout.ms`.
	SessionTimeout time.Duration

	// SSL contains all configuration values for Kafka SSL connections. Defaults
	// to an empty struct, meaning no SSL configuration is required to connect to
	// the brokers.
	SSL SSL

	// Topics is a list of topics to which to subscribe for this consumer.
	Topics []string
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	CommitInterval:    5 * time.Second,
	Debug:             Debug{},
	HeartbeatInterval: 10 * time.Second,
	InitialOffset:     OffsetBeginning,
	Logger:            *zap.NewNop(),
	SessionTimeout:    30 * time.Second,
	SSL:               SSL{},
}
