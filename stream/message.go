package stream

import (
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Message send to or received from a stream.
type Message struct {
	Value         []byte
	Key           []byte
	Timestamp     time.Time
	kafkaMessage  *sarama.ConsumerMessage
	kafkaConsumer *cluster.Consumer
}

// NewMessageFromKafka creates a new message with required Kafka metadata.
func NewMessageFromKafka(msg *sarama.ConsumerMessage, c *cluster.Consumer) *Message {

	// TODO: This was needed to prevent message modifications to cross-contaminate
	//       the next message. Haven't added test yet, but this fixes that issue.
	value := make([]byte, len(msg.Value))
	copy(value, msg.Value)

	key := make([]byte, len(msg.Key))
	copy(key, msg.Key)

	return &Message{
		Value:         value,
		Key:           key,
		Timestamp:     msg.Timestamp,
		kafkaMessage:  msg,
		kafkaConsumer: c,
	}
}

// Done tells the consumer a message has been processed, and should not be send
// again.
func (m *Message) Done() {
	if m.kafkaConsumer == nil || m.kafkaMessage == nil {
		return
	}

	m.kafkaConsumer.MarkOffset(m.kafkaMessage, "")
}
