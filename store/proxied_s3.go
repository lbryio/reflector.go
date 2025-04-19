package store

import (
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/stream"
)

// ProxiedS3Store writes to an S3 store and reads from any BlobStore (usually an ITTTStore of HttpStore endpoints).
type ProxiedS3Store struct {
	cf BlobStore
	s3 *S3Store
}

// NewProxiedS3Store returns an initialized ProxiedS3Store store pointer.
// NOTE: It panics if either argument is nil.
func NewProxiedS3Store(cf BlobStore, s3 *S3Store) *ProxiedS3Store {
	if cf == nil || s3 == nil {
		panic("both stores must be set")
	}
	return &ProxiedS3Store{cf: cf, s3: s3}
}

const nameProxiedS3 = "proxied-s3"

// Name is the cache type name
func (c *ProxiedS3Store) Name() string { return nameProxiedS3 }

// Has checks if the hash is in the store.
func (c *ProxiedS3Store) Has(hash string) (bool, error) {
	return c.cf.Has(hash)
}

// Get gets the blob from Cloudfront.
func (c *ProxiedS3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.cf.Get(hash)
	return blob, trace.Stack(time.Since(start), c.Name()), err
}

// Put stores the blob on S3
func (c *ProxiedS3Store) Put(hash string, blob stream.Blob) error {
	return c.s3.Put(hash, blob)
}

// PutSD stores the sd blob on S3
func (c *ProxiedS3Store) PutSD(hash string, blob stream.Blob) error {
	return c.s3.PutSD(hash, blob)
}

// Delete deletes the blob from S3
func (c *ProxiedS3Store) Delete(hash string) error {
	return c.s3.Delete(hash)
}

// Shutdown shuts down the store gracefully
func (c *ProxiedS3Store) Shutdown() {
	c.s3.Shutdown()
	c.cf.Shutdown()
}
