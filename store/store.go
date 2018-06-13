package store

import "github.com/lbryio/lbry.go/errors"

// BlobStore is an interface with methods for consistently handling blob storage.
type BlobStore interface {
	Has(string) (bool, error)
	Get(string) ([]byte, error)
	Put(string, []byte) error
	PutSD(string, []byte) error
}

//ErrBlobNotFound is a standard error when a blob is not found in the store.
var ErrBlobNotFound = errors.Base("blob not found")
