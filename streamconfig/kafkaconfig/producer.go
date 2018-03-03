package kafkaconfig

import (
	"time"

	"go.uber.org/zap"
)

// Producer is a value-object, containing all user-configurable configuration
// values that dictate how the Kafka client's producer will behave.
type Producer struct {
	// Brokers is a list of host/port pairs to use for establishing the initial
	// connection to the Kafka cluster. The client will make use of all servers
	// irrespective of which servers are specified here for bootstrapping — this
	// list only impacts the initial hosts used to discover the full set of
	// servers. Since these servers are just used for the initial connection to
	// discover the full cluster membership (which may change dynamically), this
	// list need not contain the full set of servers (you may want more than one,
	// though, in case a server is down).
	Brokers []string

	// Debug allows tweaking of the default debug values.
	Debug Debug

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

	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger

	// MaxDeliveryRetries dictates how many times to retry sending a failing
	// MessageSet. Note: retrying may cause reordering. Defaults to 0 retries to
	// prevent accidental message reordering.
	MaxDeliveryRetries int

	// MaxQueueBufferDuration is the delay to wait for messages in the producer
	// queue to accumulate before constructing message batches (MessageSets) to
	// transmit to brokers. A higher value allows larger and more effective (less
	// overhead, improved compression) batches of messages to accumulate at the
	// expense of increased message delivery latency. Defaults to 0.
	MaxQueueBufferDuration time.Duration

	// MaxQueueSize dictates the maximum number of messages allowed on the
	// producer queue.
	MaxQueueSize int

	// RequiredAcks indicates how many acknowledgements the leader broker must
	// receive from ISR brokers before responding to the request:
	//
	// AckNone: Broker does not send any response/ack to client
	// AckLeader: Only the leader broker will need to ack the message,
	// AckAll: broker will block until message is committed by all in sync
	// replicas (ISRs).
	//
	// Defaults to `AckLeader`.
	RequiredAcks Ack

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

	// Topic is the topic used to deliver messages to. This value is used as a
	// default value, if the provided message does not define a topic of its own.
	Topic string
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Debug:                  Debug{},
	HeartbeatInterval:      10 * time.Second,
	Logger:                 *zap.NewNop(),
	MaxDeliveryRetries:     0,
	MaxQueueBufferDuration: time.Duration(0),
	MaxQueueSize:           10000000,
	RequiredAcks:           AckLeader,
	SessionTimeout:         30 * time.Second,
	SSL:                    SSL{},
}
