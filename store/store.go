package store

import "github.com/lbryio/lbry.go/errors"

// BlobStore is an interface with methods for consistently handling blob storage.
type BlobStore interface {
	// Does blob exist in the store
	Has(hash string) (bool, error)
	// Get the blob from the store
	Get(hase string) ([]byte, error)
	// Put the blob into the store
	Put(hash string, blob []byte) error
	// Put an SD blob into the store
	PutSD(hash string, blob []byte) error
	// Delete the blob from the store
	Delete(hash string) error
}

type Blocklister interface {
	// Block deletes the blob and prevents it from being uploaded in the future
	Block(hash string) error
	// Wants returns false if the hash exists or is blocked, true otherwise
	Wants(hash string) (bool, error)
}

//ErrBlobNotFound is a standard error when a blob is not found in the store.
var ErrBlobNotFound = errors.Base("blob not found")
