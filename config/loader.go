package config

import (
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/store"
	"github.com/spf13/viper"
)

func LoadStores(configFile string) (map[string]store.BlobStore, error) {
	v := viper.New()
	v.SetConfigFile(configFile)
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	}

	stores := make(map[string]store.BlobStore)
	for storeType := range v.AllSettings() {
		factory, exists := store.Factories[storeType]
		if !exists {
			return nil, errors.Err("unknown store type: %s", storeType)
		}
		storeConfig := v.Sub(storeType)
		s, err := factory(storeConfig)
		if err != nil {
			return nil, errors.Err(err)
		}
		stores[storeType] = s
	}
	return stores, nil
}
