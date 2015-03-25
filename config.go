package gocelery

// Config stores the configuration information for gocelery
type Config struct {
	// BrokerURL in the format amqp:user@password//<host>/<virtualhost>
	BrokerURL string
	// LogLevel: debug, info, warn, error, fatal
	LogLevel string
}
