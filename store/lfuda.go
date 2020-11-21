package store

import (
	"github.com/bparli/lfuda-go"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/internal/metrics"
)

// LRUStore adds a max cache size and LRU eviction to a BlobStore
type LFUDAStore struct {
	// underlying store
	store BlobStore
	// lfuda implementation
	lfuda *lfuda.Cache
}

// NewLRUStore initialize a new LRUStore
func NewLFUDAStore(component string, store BlobStore, maxSize float64) *LFUDAStore {
	lfuda := lfuda.NewGDSFWithEvict(maxSize, func(key interface{}, value interface{}) {
		metrics.CacheLRUEvictCount.With(metrics.CacheLabels(store.Name(), component)).Inc()
		_ = store.Delete(key.(string)) // TODO: log this error. may happen if underlying entry is gone but cache entry still there
	})
	lfuda.Age()
	l := &LFUDAStore{
		store: store,
		lfuda: lfuda,
	}
	go func() {
		if lstr, ok := store.(lister); ok {
			err := l.loadExisting(lstr, int(maxSize/2000000.0))
			if err != nil {
				panic(err) // TODO: what should happen here? panic? return nil? just keep going?
			}
		}
	}()

	return l
}

const nameLFUDA = "lfuda"

var fakeTrue = []byte{'t'}

// Name is the cache type name
func (l *LFUDAStore) Name() string { return nameLFUDA }

// Has returns whether the blob is in the store, without updating the recent-ness.
func (l *LFUDAStore) Has(hash string) (bool, error) {
	return l.lfuda.Contains(hash), nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (l *LFUDAStore) Get(hash string) (stream.Blob, error) {
	_, has := l.lfuda.Get(hash)
	if !has {
		return nil, errors.Err(ErrBlobNotFound)
	}
	blob, err := l.store.Get(hash)
	if errors.Is(err, ErrBlobNotFound) {
		// Blob disappeared from underlying store
		l.lfuda.Remove(hash)
	}
	return blob, err
}

// Put stores the blob. Following LFUDA rules it's not guaranteed that a SET will store the value!!!
func (l *LFUDAStore) Put(hash string, blob stream.Blob) error {
	err := l.store.Put(hash, blob)
	if err != nil {
		return err
	}
	l.lfuda.Set(hash, fakeTrue)
	return nil
}

// PutSD stores the sd blob
func (l *LFUDAStore) PutSD(hash string, blob stream.Blob) error {
	err := l.store.PutSD(hash, blob)
	if err != nil {
		return err
	}

	l.lfuda.Set(hash, fakeTrue)
	return nil
}

// Delete deletes the blob from the store
func (l *LFUDAStore) Delete(hash string) error {
	err := l.store.Delete(hash)
	if err != nil {
		return err
	}

	// This must come after store.Delete()
	// Remove triggers onEvict function, which also tries to delete blob from store
	// We need to delete it manually first so any errors can be propagated up
	l.lfuda.Remove(hash)
	return nil
}

// loadExisting imports existing blobs from the underlying store into the LRU cache
func (l *LFUDAStore) loadExisting(store lister, maxItems int) error {
	existing, err := store.list()
	if err != nil {
		return err
	}

	added := 0
	for _, h := range existing {
		l.lfuda.Set(h, fakeTrue)
		added++
		if maxItems > 0 && added >= maxItems { // underlying cache is bigger than LRU cache
			break
		}
	}
	return nil
}
