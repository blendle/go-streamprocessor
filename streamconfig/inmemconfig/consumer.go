package inmemconfig

// Consumer is a value-type, containing all user-configurable configuration
// values that dictate how a inmem client's consumer will behave.
type Consumer struct {
}

// consumerDefaults holds the default values for Consumer.
var consumerDefaults = Consumer{}

// ConsumerDefaults returns the provided defaults, optionally using pre-defined
// client defaults to build the final defaults struct.
func ConsumerDefaults(c Client) Consumer {
	config := consumerDefaults
	return config
}
