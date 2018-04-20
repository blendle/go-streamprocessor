package pubsubconfig

import "go.uber.org/zap"

// Consumer is a value-type, containing all user-configurable configuration
// values that dictate how a Pubsub client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger zap.Logger
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger: *zap.NewNop(),
}
