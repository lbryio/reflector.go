package server

// BlobServer defines the common interface for all blob server implementations
type BlobServer interface {
	Start() error
	Shutdown()
}

type BlobServerConfig struct {
	Address               string `mapstructure:"address"`
	EdgeToken             string `mapstructure:"edge_token"`
	Port                  int    `mapstructure:"port"`
	MaxConcurrentRequests int    `mapstructure:"max_concurrent_requests"`
}
