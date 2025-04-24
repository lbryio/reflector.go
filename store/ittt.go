package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/spf13/viper"
)

// ITTTStore performs an operation on this storage, if this fails, it attempts to run it on that
type ITTTStore struct {
	this, that BlobStore
	name       string
}

type ITTTParams struct {
	Name string    `mapstructure:"name"`
	This BlobStore `mapstructure:"this"`
	That BlobStore `mapstructure:"that"`
}

type ITTTConfig struct {
	Name string `mapstructure:"name"`
	This *viper.Viper
	That *viper.Viper
}

// NewITTTStore returns a new instance of the IF THIS THAN THAT store
func NewITTTStore(params ITTTParams) *ITTTStore {
	return &ITTTStore{
		this: params.This,
		that: params.That,
		name: params.Name,
	}
}

const nameIttt = "ittt"

func ITTTStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg ITTTConfig
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	cfg.This = config.Sub("this")
	cfg.That = config.Sub("that")

	thisStoreType := strings.Split(cfg.This.AllKeys()[0], ".")[0]
	thisStoreConfig := cfg.This.Sub(thisStoreType)
	factory, ok := Factories[thisStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", thisStoreType)
	}
	thisStore, err := factory(thisStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	thatStoreType := strings.Split(cfg.That.AllKeys()[0], ".")[0]
	thatStoreConfig := cfg.That.Sub(thatStoreType)
	factory, ok = Factories[thatStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", thatStoreType)
	}
	thatStore, err := factory(thatStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	return NewITTTStore(ITTTParams{
		Name: cfg.Name,
		This: thisStore,
		That: thatStore,
	}), nil
}

func init() {
	RegisterStore(nameIttt, ITTTStoreFactory)
}

// Name is the cache type name
func (c *ITTTStore) Name() string { return nameIttt + "-" + c.name }

// Has checks in this for a hash, if it fails it checks in that. It returns true if either store has it.
func (c *ITTTStore) Has(hash string) (bool, error) {
	has, err := c.this.Has(hash)
	if err != nil || !has {
		has, err = c.that.Has(hash)
	}
	return has, err
}

// Get tries to get the blob from this first, falling back to that.
func (c *ITTTStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.this.Get(hash)
	if err == nil {
		metrics.ThisHitCount.Inc()
		return blob, trace.Stack(time.Since(start), c.Name()), err
	}

	blob, trace, err = c.that.Get(hash)
	if err != nil {
		return nil, trace.Stack(time.Since(start), c.Name()), err
	}
	metrics.ThatHitCount.Inc()
	return blob, trace.Stack(time.Since(start), c.Name()), nil
}

// Put not implemented
func (c *ITTTStore) Put(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// PutSD not implemented
func (c *ITTTStore) PutSD(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Delete not implemented
func (c *ITTTStore) Delete(hash string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown shuts down the store gracefully
func (c *ITTTStore) Shutdown() {
	c.this.Shutdown()
	c.that.Shutdown()
}
