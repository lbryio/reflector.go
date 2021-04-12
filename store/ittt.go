package store

import (
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"
)

// ITTT store performs an operation on this storage, if this fails, it attempts to run it on that
type ITTTStore struct {
	this, that BlobStore
}

// NewCachingStore makes a new caching disk store and returns a pointer to it.
func NewITTTStore(this, that BlobStore) *ITTTStore {
	return &ITTTStore{
		this: this,
		that: that,
	}
}

const nameIttt = "ittt"

// Name is the cache type name
func (c *ITTTStore) Name() string { return nameIttt }

// Has checks the cache and then the origin for a hash. It returns true if either store has it.
func (c *ITTTStore) Has(hash string) (bool, error) {
	has, err := c.this.Has(hash)
	if err != nil || !has {
		has, err = c.that.Has(hash)
	}
	return has, err
}

// Get tries to get the blob from this first, falling back to that.
func (c *ITTTStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.this.Get(hash)
	if err == nil {
		metrics.ThisHitCount.Inc()
		return blob, trace.Stack(time.Since(start), c.Name()), err
	}

	blob, trace, err = c.that.Get(hash)
	if err != nil {
		return nil, trace.Stack(time.Since(start), c.Name()), err
	}
	metrics.ThatHitCount.Inc()
	return blob, trace.Stack(time.Since(start), c.Name()), nil
}

// Put not implemented
func (c *ITTTStore) Put(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// PutSD not implemented
func (c *ITTTStore) PutSD(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Delete not implemented
func (c *ITTTStore) Delete(hash string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown shuts down the store gracefully
func (c *ITTTStore) Shutdown() {
	return
}
