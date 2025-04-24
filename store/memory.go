package store

import (
	"sync"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/spf13/viper"
)

// MemStore is an in memory only blob store with no persistence.
type MemStore struct {
	blobs map[string]stream.Blob
	mu    *sync.RWMutex
	name  string
}

type MemParams struct {
	Name string `mapstructure:"name"`
}

func NewMemStore(params MemParams) *MemStore {
	return &MemStore{
		blobs: make(map[string]stream.Blob),
		mu:    &sync.RWMutex{},
		name:  params.Name,
	}
}

const nameMem = "mem"

func MemStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg MemParams
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewMemStore(cfg), nil
}

func init() {
	RegisterStore(nameMem, MemStoreFactory)
}

// Name is the cache type name
func (m *MemStore) Name() string { return nameMem + "-" + m.name }

// Has returns T/F if the blob is currently stored. It will never error.
func (m *MemStore) Has(hash string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.blobs[hash]
	return ok, nil
}

// Get returns the blob byte slice if present and errors if the blob is not found.
func (m *MemStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	m.mu.RLock()
	defer m.mu.RUnlock()
	blob, ok := m.blobs[hash]
	if !ok {
		return nil, shared.NewBlobTrace(time.Since(start), m.Name()), errors.Err(ErrBlobNotFound)
	}
	return blob, shared.NewBlobTrace(time.Since(start), m.Name()), nil
}

// Put stores the blob in memory
func (m *MemStore) Put(hash string, blob stream.Blob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[hash] = blob
	return nil
}

// PutSD stores the sd blob in memory
func (m *MemStore) PutSD(hash string, blob stream.Blob) error {
	return m.Put(hash, blob)
}

// Delete deletes the blob from the store
func (m *MemStore) Delete(hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.blobs, hash)
	return nil
}

// Debug returns the blobs in memory. It's useful for testing and debugging.
func (m *MemStore) Debug() map[string]stream.Blob {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.blobs
}

// Shutdown shuts down the store gracefully
func (m *MemStore) Shutdown() {}
