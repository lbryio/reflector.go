package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// CachingStore combines two stores, typically a local and a remote store, to improve performance.
// Accessed blobs are stored in and retrieved from the cache. If they are not in the cache, they
// are retrieved from the origin and cached. Puts are cached and also forwarded to the origin.
type CachingStore struct {
	origin, cache BlobStore
	name          string
}

type CachingParams struct {
	Origin BlobStore `mapstructure:"origin"`
	Cache  BlobStore `mapstructure:"cache"`
	Name   string    `mapstructure:"name"`
}

type CachingConfig struct {
	Origin *viper.Viper
	Cache  *viper.Viper
	Name   string `mapstructure:"name"`
}

// NewCachingStore makes a new caching disk store and returns a pointer to it.
func NewCachingStore(params CachingParams) *CachingStore {
	return &CachingStore{
		name:   params.Name,
		origin: WithSingleFlight(params.Name, params.Origin),
		cache:  WithSingleFlight(params.Name, params.Cache),
	}
}

const nameCaching = "caching"

func CachingStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg CachingConfig
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	cfg.Cache = config.Sub("cache")
	cfg.Origin = config.Sub("origin")
	if cfg.Cache == nil || cfg.Origin == nil {
		return nil, errors.Err("cache and origin missing")
	}

	originStoreType := strings.Split(cfg.Origin.AllKeys()[0], ".")[0]
	originStoreConfig := cfg.Origin.Sub(originStoreType)
	factory, ok := Factories[originStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", originStoreType)
	}
	originStore, err := factory(originStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	cacheStoreType := strings.Split(cfg.Cache.AllKeys()[0], ".")[0]
	cacheStoreConfig := cfg.Cache.Sub(cacheStoreType)
	factory, ok = Factories[cacheStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", cacheStoreType)
	}
	cacheStore, err := factory(cacheStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	return NewCachingStore(CachingParams{
		Name:   cfg.Name,
		Origin: originStore,
		Cache:  cacheStore,
	}), nil
}

func init() {
	RegisterStore(nameCaching, CachingStoreFactory)
}

// Name is the cache type name
func (c *CachingStore) Name() string { return nameCaching }

// Has checks the cache and then the origin for a hash. It returns true if either store has it.
func (c *CachingStore) Has(hash string) (bool, error) {
	has, err := c.cache.Has(hash)
	if has || err != nil {
		return has, err
	}
	return c.origin.Has(hash)
}

// Get tries to get the blob from the cache first, falling back to the origin. If the blob comes
// from the origin, it is also stored in the cache.
func (c *CachingStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.cache.Get(hash)
	if err == nil || !errors.Is(err, ErrBlobNotFound) {
		metrics.CacheHitCount.With(metrics.CacheLabels(c.cache.Name(), c.name)).Inc()
		rate := float64(len(blob)) / 1024 / 1024 / time.Since(start).Seconds()
		metrics.CacheRetrievalSpeed.With(map[string]string{
			metrics.LabelCacheType: c.cache.Name(),
			metrics.LabelComponent: c.name,
			metrics.LabelSource:    "cache",
		}).Set(rate)
		return blob, trace.Stack(time.Since(start), c.Name()), err
	}

	metrics.CacheMissCount.With(metrics.CacheLabels(c.cache.Name(), c.name)).Inc()

	blob, trace, err = c.origin.Get(hash)
	if err != nil {
		return nil, trace.Stack(time.Since(start), c.Name()), err
	}
	// do not do this async unless you're prepared to deal with mayhem
	err = c.cache.Put(hash, blob)
	if err != nil {
		log.Errorf("error saving blob to underlying cache: %s", errors.FullTrace(err))
	}
	return blob, trace.Stack(time.Since(start), c.Name()), nil
}

// Put stores the blob in the origin and the cache
func (c *CachingStore) Put(hash string, blob stream.Blob) error {
	err := c.origin.Put(hash, blob)
	if err != nil {
		return err
	}
	return c.cache.Put(hash, blob)
}

// PutSD stores the sd blob in the origin and the cache
func (c *CachingStore) PutSD(hash string, blob stream.Blob) error {
	err := c.origin.PutSD(hash, blob)
	if err != nil {
		return err
	}
	return c.cache.PutSD(hash, blob)
}

// Delete deletes the blob from the origin and the cache
func (c *CachingStore) Delete(hash string) error {
	err := c.origin.Delete(hash)
	if err != nil {
		return err
	}
	return c.cache.Delete(hash)
}

// Shutdown shuts down the store gracefully
func (c *CachingStore) Shutdown() {
	c.origin.Shutdown()
	c.cache.Shutdown()
}
