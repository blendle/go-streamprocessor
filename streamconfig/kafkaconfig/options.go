package kafkaconfig

import (
	"reflect"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

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

// ConfigValue returns the kafka.ConfigValue value for the method receiver.
func (d Debug) ConfigValue() kafka.ConfigValue {
	if d.All {
		return kafka.ConfigValue("all")
	}

	debugs := make([]string, 0)
	v := reflect.ValueOf(d)

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Bool() {
			debugs = append(debugs, strings.ToLower(v.Type().Field(i).Name))
		}
	}

	return kafka.ConfigValue(strings.Join(debugs, ","))
}

// Set is used by the `envconfig` package to allow setting the debug
// configuration through environment variables.
func (d *Debug) Set(value string) error {
	v := strings.Split(strings.ToLower(value), ",")

	// If any of the values is "all" or "true", set Debug.All to `true` and return
	// early.
	for i := range v {
		if v[i] == "all" || v[i] == "true" || v[i] == "1" {
			d.All = true
			return nil
		}
	}

	// Else, we go through all available Debug struct fields, and set the value to
	// true if we find a matching name in the provided environment variable value.
	rv := reflect.ValueOf(d).Elem()
	t := rv.Type()
	for i := 0; i < t.NumField(); i++ {
		for j := range v {
			if v[j] == strings.ToLower(t.Field(i).Name) {
				rv.Field(i).SetBool(true)
			}
		}
	}

	return nil
}

// SSL contains all configuration values for Kafka SSL connections.
type SSL struct {
	// CAPath is the file or directory path to CA certificate(s) for verifying the
	// broker's key.
	CAPath string `kafka:"ca.location,omitempty" envconfig:"ca_path"`

	// CertPath is the path to client's public key (PEM) used for authentication.
	CertPath string `kafka:"certificate.location,omitempty" envconfig:"cert_path"`

	// CRLPath is the path to CRL for verifying broker's certificate validity.
	CRLPath string `kafka:"crl.location,omitempty" envconfig:"crl_path"`

	// KeyPassword is the password of the private key in the key store file. This
	// is optional for client.
	KeyPassword string `kafka:"key.password,omitempty" split_words:"true"`

	// KeyPath is the path to client's private key (PEM) used for authentication.
	KeyPath string `kafka:"key.location,omitempty" split_words:"true"`

	// KeystorePassword is the store password for the key store file. This is
	// optional for client and only needed if `KeystorePath` is configured.
	KeystorePassword string `kafka:"keystore.password,omitempty" split_words:"true"`

	// KeystorePath is the location of the key store file. This is optional for
	// client and can be used for two-way authentication for client.
	KeystorePath string `kafka:"keystore.location,omitempty" split_words:"true"`
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

// ConfigValue returns the kafka.ConfigValue value for the method receiver.
func (o Offset) ConfigValue() kafka.ConfigValue {
	return string(o)
}

// Set is used by the `envconfig` package to set the right value based off of
// the provided environment variables.
func (o *Offset) Set(value string) error {
	*o = Offset(strings.ToLower(value))
	return nil
}

// Ack is the configuration that dictates the acknowledgment behavior of the
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

// ConfigValue returns the kafka.ConfigValue value for the method receiver.
func (a Ack) ConfigValue() kafka.ConfigValue {
	return int(a)
}

// Protocol is the protocol used to communicate with brokers.
type Protocol string

const (
	// ProtocolPlaintext is the default unencrypted protocol.
	ProtocolPlaintext Protocol = "plaintext"

	// ProtocolSSL is the SSL-based protocol.
	ProtocolSSL Protocol = "ssl"

	// ProtocolSASLPlaintext is the Simple Authentication and Security Layer
	// protocol with plain text.
	ProtocolSASLPlaintext Protocol = "sasl_plaintext"

	// ProtocolSASLSSL is the Simple Authentication and Security Layer protocol
	// with SSL.
	ProtocolSASLSSL Protocol = "sasl_ssl"
)

// ConfigValue returns the kafka.ConfigValue value for the method receiver.
func (p Protocol) ConfigValue() kafka.ConfigValue {
	return string(p)
}

// Set is used by the `envconfig` package to set the right value based off of
// the provided environment variables.
func (p *Protocol) Set(value string) error {
	*p = Protocol(strings.ToLower(value))
	return nil
}

// Compression is the compression codec used to compress message sets before
// delivering them to the Kafka brokers.
type Compression string

const (
	// CompressionNone sets no message compression. Less CPU usage, more data
	// volume to transmit, slower throughput.
	CompressionNone Compression = "none"

	// CompressionGZIP sets message compression to GZIP.
	CompressionGZIP Compression = "gzip"

	// CompressionSnappy sets message compression to Snappy.
	CompressionSnappy Compression = "snappy"

	// CompressionLZ4 sets message compression to LZ4.
	CompressionLZ4 Compression = "lz4"
)

// ConfigValue returns the kafka.ConfigValue value for the method receiver.
func (c Compression) ConfigValue() kafka.ConfigValue {
	return string(c)
}
