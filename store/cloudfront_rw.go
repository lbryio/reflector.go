package store

import (
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/stream"
)

// CloudFrontRWStore combines a Cloudfront and an S3 store. Reads go to Cloudfront/Wasabi, writes go to S3.
type CloudFrontRWStore struct {
	cf *ITTTStore
	s3 *S3Store
}

// NewCloudFrontRWStore returns an initialized CloudFrontRWStore store pointer.
// NOTE: It panics if either argument is nil.
func NewCloudFrontRWStore(cf *ITTTStore, s3 *S3Store) *CloudFrontRWStore {
	if cf == nil || s3 == nil {
		panic("both stores must be set")
	}
	return &CloudFrontRWStore{cf: cf, s3: s3}
}

const nameCloudFrontRW = "cloudfront_rw"

// Name is the cache type name
func (c *CloudFrontRWStore) Name() string { return nameCloudFrontRW }

// Has checks if the hash is in the store.
func (c *CloudFrontRWStore) Has(hash string) (bool, error) {
	return c.cf.Has(hash)
}

// Get gets the blob from Cloudfront.
func (c *CloudFrontRWStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.cf.Get(hash)
	return blob, trace.Stack(time.Since(start), c.Name()), err
}

// Put stores the blob on S3
func (c *CloudFrontRWStore) Put(hash string, blob stream.Blob) error {
	return c.s3.Put(hash, blob)
}

// PutSD stores the sd blob on S3
func (c *CloudFrontRWStore) PutSD(hash string, blob stream.Blob) error {
	return c.s3.PutSD(hash, blob)
}

// Delete deletes the blob from S3
func (c *CloudFrontRWStore) Delete(hash string) error {
	return c.s3.Delete(hash)
}

// Shutdown shuts down the store gracefully
func (c *CloudFrontRWStore) Shutdown() {
	c.s3.Shutdown()
	c.cf.Shutdown()
}
