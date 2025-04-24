package store

import (
	"github.com/lbryio/reflector.go/shared"
	"github.com/spf13/viper"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// BlobStore is an interface for handling blob storage.
type BlobStore interface {
	// Name of blob store (useful for metrics)
	Name() string
	// Has Does blob exist in the store.
	Has(hash string) (bool, error)
	// Get the blob from the store. Must return ErrBlobNotFound if blob is not in store.
	Get(hash string) (stream.Blob, shared.BlobTrace, error)
	// Put the blob into the store.
	Put(hash string, blob stream.Blob) error
	// PutSD an SD blob into the store.
	PutSD(hash string, blob stream.Blob) error
	// Delete the blob from the store.
	Delete(hash string) error
	// Shutdown the store gracefully
	Shutdown()
}

// Blocklister is a store that supports blocking blobs to prevent their inclusion in the store.
type Blocklister interface {
	// Block deletes the blob and prevents it from being uploaded in the future
	Block(hash string) error
	// Wants returns false if the hash exists in store or is blocked, true otherwise
	Wants(hash string) (bool, error)
}

// lister is a store that can list cached blobs. This is helpful when an overlay
// cache needs to track blob existence.
type lister interface {
	list() ([]string, error)
}

// ErrBlobNotFound is a standard error when a blob is not found in the store.
var ErrBlobNotFound = errors.Base("blob not found")
var Factories = make(map[string]Factory)

func RegisterStore(name string, factory Factory) {
	Factories[name] = factory
}

type Factory func(config *viper.Viper) (BlobStore, error)
