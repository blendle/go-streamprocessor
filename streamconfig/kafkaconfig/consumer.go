package kafkaconfig

import (
	"go.uber.org/zap"
)

// Consumer is a value-object, containing all user-configurable configuration
// values that dictate how the Kafka client's consumer will behave.
type Consumer struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a no-op logger will be used.
	Logger *zap.Logger
}

// ConsumerDefaults holds the default values for Consumer.
var ConsumerDefaults = Consumer{
	Logger: zap.NewNop(),
}
