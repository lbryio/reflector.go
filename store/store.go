package store

import "github.com/lbryio/lbry.go/errors"

type BlobStore interface {
	Has(string) (bool, error)
	Get(string) ([]byte, error)
	Put(string, []byte) error
	PutSD(string, []byte) error
}

var ErrBlobNotFound = errors.Base("blob not found")
