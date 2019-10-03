package store

import (
	"github.com/lbryio/lbry.go/extras/errors"
	"github.com/lbryio/lbry.go/stream"
)

// MemoryBlobStore is an in memory only blob store with no persistence.
type MemoryBlobStore struct {
	blobs map[string][]byte
}

// Has returns T/F if the blob is currently stored. It will never error.
func (m *MemoryBlobStore) Has(hash string) (bool, error) {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	_, ok := m.blobs[hash]
	return ok, nil
}

// Get returns the blob byte slice if present and errors if the blob is not found.
func (m *MemoryBlobStore) Get(hash string) (stream.Blob, error) {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	blob, ok := m.blobs[hash]
	if !ok {
		return []byte{}, errors.Err(ErrBlobNotFound)
	}
	return blob, nil
}

// Put stores the blob in memory
func (m *MemoryBlobStore) Put(hash string, blob stream.Blob) error {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	m.blobs[hash] = blob
	return nil
}

// PutSD stores the sd blob in memory
func (m *MemoryBlobStore) PutSD(hash string, blob stream.Blob) error {
	return m.Put(hash, blob)
}

// Delete deletes the blob from the store
func (m *MemoryBlobStore) Delete(hash string) error {
	delete(m.blobs, hash)
	return nil
}

// Debug returns the blobs in memory. It's useful for testing and debugging.
func (m *MemoryBlobStore) Debug() map[string][]byte {
	return m.blobs
}
