package kafkaconfig

// Producer is a value-type, containing all user-configurable configuration
// values that dictate how a Kafka client's producer will behave.
type Producer struct {
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{}
