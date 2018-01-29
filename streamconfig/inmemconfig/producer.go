package inmemconfig

// Producer is a value-type, containing all user-configurable configuration
// values that dictate how a inmem client's producer will behave.
type Producer struct {
}

// producerDefaults holds the default values for Producer.
var producerDefaults = Producer{}

// ProducerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ProducerDefaults(c Client) Producer {
	config := producerDefaults
	return config
}
