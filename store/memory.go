package store

import "github.com/lbryio/lbry.go/errors"

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
func (m *MemoryBlobStore) Get(hash string) ([]byte, error) {
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
func (m *MemoryBlobStore) Put(hash string, blob []byte) error {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	m.blobs[hash] = blob
	return nil
}

// PutSD stores the sd blob in memory
func (m *MemoryBlobStore) PutSD(hash string, blob []byte) error {
	return m.Put(hash, blob)
}
