package store

import (
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// BlobStore is an interface for handling blob storage.
type BlobStore interface {
	// Does blob exist in the store
	Has(hash string) (bool, error)
	// Get the blob from the store
	Get(hash string) (stream.Blob, error)
	// Put the blob into the store
	Put(hash string, blob stream.Blob) error
	// Put an SD blob into the store
	PutSD(hash string, blob stream.Blob) error
	// Delete the blob from the store
	Delete(hash string) error
}

// Blocklister is a store that supports blocking blobs to prevent their inclusion in the store.
type Blocklister interface {
	// Block deletes the blob and prevents it from being uploaded in the future
	Block(hash string) error
	// Wants returns false if the hash exists in store or is blocked, true otherwise
	Wants(hash string) (bool, error)
}

//ErrBlobNotFound is a standard error when a blob is not found in the store.
var ErrBlobNotFound = errors.Base("blob not found")
