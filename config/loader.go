package config

import (
	"fmt"

	"github.com/lbryio/reflector.go/server"
	"github.com/lbryio/reflector.go/server/http"
	"github.com/lbryio/reflector.go/server/http3"
	"github.com/lbryio/reflector.go/server/peer"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/spf13/viper"
)

func LoadStores(configFile string) (store.BlobStore, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	storeViper := v.Sub("store")
	for storeType := range storeViper.AllSettings() {
		factory, exists := store.Factories[storeType]
		if !exists {
			return nil, errors.Err("unknown store type: %s", storeType)
		}
		storeConfig := storeViper.Sub(storeType)
		s, err := factory(storeConfig)
		if err != nil {
			return nil, errors.Err(err)
		}
		//we only expect 1 store as the root, so let's return it
		return s, nil
	}
	return nil, nil
}

func LoadServers(store store.BlobStore, configFile string) ([]server.BlobServer, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	servers := make([]server.BlobServer, 0)
	serversViper := v.Sub("servers")
	for serverType := range serversViper.AllSettings() {
		var cfg server.BlobServerConfig
		err := serversViper.Sub(serverType).Unmarshal(&cfg)
		if err != nil {
			return nil, errors.Err(err)
		}
		switch serverType {
		case "http":
			servers = append(servers, http.NewServer(store, cfg.MaxConcurrentRequests, cfg.EdgeToken, fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)))
		case "http3":
			servers = append(servers, http3.NewServer(store, cfg.MaxConcurrentRequests, fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)))
		case "peer":
			servers = append(servers, peer.NewServer(store, fmt.Sprintf("%s:%d", cfg.Address, cfg.Port)))
		default:
			return nil, errors.Err("unknown server type: %s", serverType)
		}
	}
	return servers, nil
}
