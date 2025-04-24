package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/bluele/gcache"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// GcacheStore adds a max cache size and Greedy-Dual-Size-Frequency cache eviction strategy to a BlobStore
type GcacheStore struct {
	underlyingStore BlobStore
	cache           gcache.Cache
	name            string
}

type EvictionStrategy int

const (
	//LFU Discards the least frequently used items first.
	LFU EvictionStrategy = iota
	//ARC Constantly balances between LRU and LFU, to improve the combined result.
	ARC
	//LRU Discards the least recently used items first.
	LRU
	//SIMPLE has no clear priority for evict cache. It depends on key-value map order.
	SIMPLE
)

type GcacheParams struct {
	Name     string           `mapstructure:"name"`
	Store    BlobStore        `mapstructure:"store"`
	MaxSize  int              `mapstructure:"max_size"`
	Strategy EvictionStrategy `mapstructure:"strategy"`
}

type GcacheConfig struct {
	Name     string `mapstructure:"name"`
	Store    *viper.Viper
	MaxSize  int              `mapstructure:"max_size"`
	Strategy EvictionStrategy `mapstructure:"strategy"`
}

// NewGcacheStore initialize a new LRUStore
func NewGcacheStore(params GcacheParams) *GcacheStore {
	cacheBuilder := gcache.New(params.MaxSize)
	var cache gcache.Cache
	evictFunc := func(key interface{}, value interface{}) {
		logrus.Infof("evicting %s", key)
		metrics.CacheLRUEvictCount.With(metrics.CacheLabels(params.Store.Name(), params.Name)).Inc()
		_ = params.Store.Delete(key.(string)) // TODO: log this error. may happen if underlying entry is gone but cache entry still there
	}
	switch params.Strategy {
	case LFU:
		cache = cacheBuilder.LFU().EvictedFunc(evictFunc).Build()
	case ARC:
		cache = cacheBuilder.ARC().EvictedFunc(evictFunc).Build()
	case LRU:
		cache = cacheBuilder.LRU().EvictedFunc(evictFunc).Build()
	case SIMPLE:
		cache = cacheBuilder.Simple().EvictedFunc(evictFunc).Build()
	}
	l := &GcacheStore{
		underlyingStore: params.Store,
		cache:           cache,
		name:            params.Name,
	}
	go func() {
		if lstr, ok := params.Store.(lister); ok {
			err := l.loadExisting(lstr, params.MaxSize)
			if err != nil {
				panic(err) // TODO: what should happen here? panic? return nil? just keep going?
			}
		}
	}()

	return l
}

const nameGcache = "gcache"

func GcacheStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg GcacheConfig
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	cfg.Store = config.Sub("store")

	storeType := strings.Split(cfg.Store.AllKeys()[0], ".")[0]
	storeConfig := cfg.Store.Sub(storeType)
	factory, ok := Factories[storeType]
	if !ok {
		return nil, errors.Err("unknown store type %s", storeType)
	}
	underlyingStore, err := factory(storeConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	return NewGcacheStore(GcacheParams{
		Name:     cfg.Name,
		Store:    underlyingStore,
		MaxSize:  cfg.MaxSize,
		Strategy: cfg.Strategy,
	}), nil
}

func init() {
	RegisterStore(nameGcache, GcacheStoreFactory)
}

// Name is the cache type name
func (l *GcacheStore) Name() string { return nameGcache + "-" + l.name }

// Has returns whether the blob is in the store, without updating the recent-ness.
func (l *GcacheStore) Has(hash string) (bool, error) {
	return l.cache.Has(hash), nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (l *GcacheStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	_, err := l.cache.Get(hash)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), l.Name()), errors.Err(ErrBlobNotFound)
	}
	blob, stack, err := l.underlyingStore.Get(hash)
	if errors.Is(err, ErrBlobNotFound) {
		// Blob disappeared from underlying store
		l.cache.Remove(hash)
	}
	return blob, stack.Stack(time.Since(start), l.Name()), err
}

// Put stores the blob. Following LFUDA rules it's not guaranteed that a SET will store the value!!!
func (l *GcacheStore) Put(hash string, blob stream.Blob) error {
	_ = l.cache.Set(hash, true)
	has, _ := l.Has(hash)
	if has {
		err := l.underlyingStore.Put(hash, blob)
		if err != nil {
			return err
		}
	}
	return nil
}

// PutSD stores the sd blob. Following LFUDA rules it's not guaranteed that a SET will store the value!!!
func (l *GcacheStore) PutSD(hash string, blob stream.Blob) error {
	_ = l.cache.Set(hash, true)
	has, _ := l.Has(hash)
	if has {
		err := l.underlyingStore.PutSD(hash, blob)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes the blob from the store
func (l *GcacheStore) Delete(hash string) error {
	err := l.underlyingStore.Delete(hash)
	if err != nil {
		return err
	}

	// This must come after store.Delete()
	// Remove triggers onEvict function, which also tries to delete blob from store
	// We need to delete it manually first so any errors can be propagated up
	l.cache.Remove(hash)
	return nil
}

// loadExisting imports existing blobs from the underlying store into the LRU cache
func (l *GcacheStore) loadExisting(store lister, maxItems int) error {
	logrus.Infof("loading at most %d items", maxItems)
	existing, err := store.list()
	if err != nil {
		return err
	}
	logrus.Infof("read %d files from underlying store", len(existing))

	added := 0
	for i, h := range existing {
		_ = l.cache.Set(h, true)
		added++
		if maxItems > 0 && added >= maxItems { // underlying cache is bigger than the cache
			err := l.Delete(h)
			logrus.Infof("deleted overflowing blob: %s (%d/%d)", h, i, len(existing))
			if err != nil {
				logrus.Warnf("error while deleting a blob that's overflowing the cache: %s", err.Error())
			}
		}
	}
	return nil
}

// Shutdown shuts down the store gracefully
func (l *GcacheStore) Shutdown() {
}
