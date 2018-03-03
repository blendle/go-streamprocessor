package kafkaconfig

// Debug contains all available debug configuration values. Each value defaults
// to `false`, but can be set to `true` accordingly.
//
// Detailed Producer debugging: broker,topic,msg.
// Consumer: consumer,cgrp,topic,fetch.
type Debug struct {
	All,

	Broker,
	CGRP,
	Consumer,
	Feature,
	Fetch,
	Generic,
	Interceptor,
	Metadata,
	Msg,
	Plugin,
	Protocol,
	Queue,
	Security,
	Topic bool
}

// SSL contains all configuration values for Kafka SSL connections.
type SSL struct {
	// KeyPassword is the password of the private key in the key store file. This
	// is optional for client.
	KeyPassword string

	// KeystorePassword is the store password for the key store file. This is
	// optional for client and only needed if `KeystorePath` is configured.
	KeystorePassword string

	// Path to client's private key (PEM) used for authentication.
	KeyPath string

	// Path to client's public key (PEM) used for authentication.
	CertPath string

	// File or directory path to CA certificate(s) for verifying the broker's key.
	CAPath string

	// Path to CRL for verifying broker's certificate validity.
	CRLPath string

	// KeystorePath is the location of the key store file. This is optional for
	// client and can be used for two-way authentication for client.
	KeystorePath string
}

// Offset is the configuration that dictates whether the consumer should start
// reading from the beginning, or the end of a group.
type Offset string

const (
	// OffsetBeginning instructs the consumer to start reading from the first
	// message.
	OffsetBeginning Offset = "beginning"

	// OffsetEnd instructs the consumer to start reading from the last message.
	OffsetEnd = "end"
)

func (o Offset) String() string {
	return string(o)
}

// Ack is the configuration that dictates the acknowledgement behavior of the
// Kafka broker to this producer.
type Ack int

const (
	// AckNone means the broker does not send any response/ack to client. High
	// throughput, low latency. No durability guarantee. The producer does not
	// wait for acknowledgment from the server.
	AckNone Ack = 0

	// AckLeader means only the leader broker will need to ack the message. Medium
	// throughput, medium latency. Leader writes the record to its local log, and
	// responds without awaiting full acknowledgment from all followers.
	AckLeader = 1

	// AckAll means the broker will block until message is committed by all in
	// sync replicas (ISRs). Low throughput, high latency. Leader waits for the
	// full set of in-sync replicas (ISRs) to acknowledge the record. This
	// guarantees that the record is not lost as long as at least one IRS is
	// active.
	AckAll = -1
)
