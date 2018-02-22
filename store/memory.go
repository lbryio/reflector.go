package store

import "github.com/lbryio/lbry.go/errors"

type MemoryBlobStore struct {
	blobs map[string][]byte
}

func (m *MemoryBlobStore) Has(hash string) (bool, error) {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	_, ok := m.blobs[hash]
	return ok, nil
}

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

func (m *MemoryBlobStore) Put(hash string, blob []byte) error {
	if m.blobs == nil {
		m.blobs = make(map[string][]byte)
	}
	m.blobs[hash] = blob
	return nil
}

func (m *MemoryBlobStore) PutSD(hash string, blob []byte) error {
	return m.Put(hash, blob)
}
