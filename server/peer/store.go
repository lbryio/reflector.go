package peer

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// Store is a blob store that gets blobs from a peer.
// It satisfies the store.BlobStore interface but cannot put or delete blobs.
type Store struct {
	opts StoreOpts
}

// StoreOpts allows to set options for a new Store.
type StoreOpts struct {
	Address string
	Timeout time.Duration
}

// NewStore makes a new peer store.
func NewStore(opts StoreOpts) *Store {
	return &Store{opts: opts}
}

func (p *Store) getClient() (*Client, error) {
	c := &Client{Timeout: p.opts.Timeout}
	err := c.Connect(p.opts.Address)
	return c, errors.Prefix("connection error", err)
}

func (p *Store) Name() string { return "peer" }

// Has asks the peer if they have a hash
func (p *Store) Has(hash string) (bool, error) {
	c, err := p.getClient()
	if err != nil {
		return false, err
	}
	defer c.Close()
	return c.HasBlob(hash)
}

// Get downloads the blob from the peer
func (p *Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	c, err := p.getClient()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), p.Name()), err
	}
	defer c.Close()
	blob, trace, err := c.GetBlob(hash)
	if err != nil && strings.Contains(err.Error(), "blob not found") {
		return nil, trace, store.ErrBlobNotFound
	}

	return blob, trace, err
}

// Put is not supported
func (p *Store) Put(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// PutSD is not supported
func (p *Store) PutSD(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Delete is not supported
func (p *Store) Delete(hash string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown is not supported
func (p *Store) Shutdown() {
}
