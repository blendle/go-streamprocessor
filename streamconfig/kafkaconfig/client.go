package kafkaconfig

import "go.uber.org/zap"

// Client is a value-type, containing all user-configurable configuration values
// that dictate how the instantiated Kafka client will behave.
type Client struct {
	// Logger is the configurable logger instance to log messages. If left
	// undefined, a noop logger will be used.
	Logger *zap.Logger
}

// clientDefaults holds the default values for Client.
var clientDefaults = Client{
	Logger: zap.NewNop(),
}

// ClientDefaults returns the provided defaults.
func ClientDefaults() Client {
	return clientDefaults
}
