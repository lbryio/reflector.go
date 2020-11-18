package store

import (
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/internal/metrics"

	golru "github.com/hashicorp/golang-lru"
)

// LRUStore adds a max cache size and LRU eviction to a BlobStore
type LRUStore struct {
	// underlying store
	store BlobStore
	// lru implementation
	lru *golru.Cache
}

// NewLRUStore initialize a new LRUStore
func NewLRUStore(component string, store BlobStore, maxItems int) *LRUStore {
	lru, err := golru.NewWithEvict(maxItems, func(key interface{}, value interface{}) {
		metrics.CacheLRUEvictCount.With(metrics.CacheLabels(store.Name(), component)).Inc()
		_ = store.Delete(key.(string)) // TODO: log this error. may happen if underlying entry is gone but cache entry still there
	})
	if err != nil {
		panic(err)
	}

	l := &LRUStore{
		store: store,
		lru:   lru,
	}
	go func() {
		if lstr, ok := store.(lister); ok {
			err = l.loadExisting(lstr, maxItems)
			if err != nil {
				panic(err) // TODO: what should happen here? panic? return nil? just keep going?
			}
		}
	}()

	return l
}

const nameLRU = "lru"

// Name is the cache type name
func (l *LRUStore) Name() string { return nameLRU }

// Has returns whether the blob is in the store, without updating the recent-ness.
func (l *LRUStore) Has(hash string) (bool, error) {
	return l.lru.Contains(hash), nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (l *LRUStore) Get(hash string) (stream.Blob, error) {
	_, has := l.lru.Get(hash)
	if !has {
		return nil, errors.Err(ErrBlobNotFound)
	}
	blob, err := l.store.Get(hash)
	if errors.Is(err, ErrBlobNotFound) {
		// Blob disappeared from underlying store
		l.lru.Remove(hash)
	}
	return blob, err
}

// Put stores the blob
func (l *LRUStore) Put(hash string, blob stream.Blob) error {
	err := l.store.Put(hash, blob)
	if err != nil {
		return err
	}

	l.lru.Add(hash, true)
	return nil
}

// PutSD stores the sd blob
func (l *LRUStore) PutSD(hash string, blob stream.Blob) error {
	err := l.store.PutSD(hash, blob)
	if err != nil {
		return err
	}

	l.lru.Add(hash, true)
	return nil
}

// Delete deletes the blob from the store
func (l *LRUStore) Delete(hash string) error {
	err := l.store.Delete(hash)
	if err != nil {
		return err
	}

	// This must come after store.Delete()
	// Remove triggers onEvict function, which also tries to delete blob from store
	// We need to delete it manually first so any errors can be propagated up
	l.lru.Remove(hash)
	return nil
}

// loadExisting imports existing blobs from the underlying store into the LRU cache
func (l *LRUStore) loadExisting(store lister, maxItems int) error {
	existing, err := store.list()
	if err != nil {
		return err
	}

	added := 0
	for _, h := range existing {
		l.lru.Add(h, true)
		added++
		if maxItems > 0 && added >= maxItems { // underlying cache is bigger than LRU cache
			break
		}
	}
	return nil
}
