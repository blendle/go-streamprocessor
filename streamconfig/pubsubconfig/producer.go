package pubsubconfig

import "go.uber.org/zap"

// Producer is a value-type, containing all user-configurable configuration
// values that dictate how a Pubsub client's producer will behave.
type Producer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger
}

// ProducerDefaults holds the default values for Producer.
var ProducerDefaults = Producer{
	Logger: *zap.NewNop(),
}
