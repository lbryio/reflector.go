package store

import (
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/bluele/gcache"
	"github.com/sirupsen/logrus"
)

// GcacheStore adds a max cache size and Greedy-Dual-Size-Frequency cache eviction strategy to a BlobStore
type GcacheStore struct {
	// underlying store
	store BlobStore
	// cache implementation
	cache gcache.Cache
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

// NewGcacheStore initialize a new LRUStore
func NewGcacheStore(component string, store BlobStore, maxSize int, strategy EvictionStrategy) *GcacheStore {
	cacheBuilder := gcache.New(maxSize)
	var cache gcache.Cache
	evictFunc := func(key interface{}, value interface{}) {
		logrus.Infof("evicting %s", key)
		metrics.CacheLRUEvictCount.With(metrics.CacheLabels(store.Name(), component)).Inc()
		_ = store.Delete(key.(string)) // TODO: log this error. may happen if underlying entry is gone but cache entry still there
	}
	switch strategy {
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
		store: store,
		cache: cache,
	}
	go func() {
		if lstr, ok := store.(lister); ok {
			err := l.loadExisting(lstr, int(maxSize))
			if err != nil {
				panic(err) // TODO: what should happen here? panic? return nil? just keep going?
			}
		}
	}()

	return l
}

const nameGcache = "gcache"

// Name is the cache type name
func (l *GcacheStore) Name() string { return nameGcache }

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
	blob, stack, err := l.store.Get(hash)
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
		err := l.store.Put(hash, blob)
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
		err := l.store.PutSD(hash, blob)
		if err != nil {
			return err
		}
	}
	return nil
}

// Delete deletes the blob from the store
func (l *GcacheStore) Delete(hash string) error {
	err := l.store.Delete(hash)
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
