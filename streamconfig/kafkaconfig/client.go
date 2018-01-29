package kafkaconfig

import "go.uber.org/zap"

// Client is a value-type, containing all user-configurable configuration values
// that dictate how the instantiated Kafka client will behave.
type Client struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a noop logger will be used.
	Logger *zap.Logger
}

// ClientDefaults holds the default values for Client.
var ClientDefaults = Client{
	Logger: zap.NewNop(),
}
