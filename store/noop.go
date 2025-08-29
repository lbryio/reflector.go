package store

import (
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/spf13/viper"
)

// NoopStore is a store that does nothing
type NoopStore struct {
	name string
}

func NewNoopStore(name string) *NoopStore {
	return &NoopStore{name: name}
}

const nameNoop = "noop"

func NoopStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg struct {
		Name string `mapstructure:"name"`
	}
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewNoopStore(cfg.Name), nil
}

func init() {
	RegisterStore(nameNoop, NoopStoreFactory)
}

func (n *NoopStore) Name() string               { return nameNoop + "-" + n.name }
func (n *NoopStore) Has(_ string) (bool, error) { return false, nil }
func (n *NoopStore) Get(_ string) (stream.Blob, shared.BlobTrace, error) {
	return nil, shared.NewBlobTrace(time.Since(time.Now()), n.Name()), nil
}
func (n *NoopStore) Put(_ string, _ stream.Blob) error   { return nil }
func (n *NoopStore) PutSD(_ string, _ stream.Blob) error { return nil }
func (n *NoopStore) Delete(_ string) error               { return nil }
func (n *NoopStore) Shutdown()                           {}
