package peer

import (
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// Store is a blob store that gets blobs from a peer.
// It satisfies the store.BlobStore interface but cannot put or delete blobs.
type Store struct {
	client  *Client
	connErr error
}

// StoreOpts allows to set options for a new Store.
type StoreOpts struct {
	Address string
	Timeout time.Duration
}

// NewStore makes a new peer store.
func NewStore(opts StoreOpts) *Store {
	if opts.Timeout == 0 {
		opts.Timeout = time.Second * 5
	}
	c := &Client{Timeout: opts.Timeout}
	err := c.Connect(opts.Address)
	return &Store{client: c, connErr: err}
}

// Has asks the peer if they have a hash
func (p *Store) Has(hash string) (bool, error) {
	if p.connErr != nil {
		return false, errors.Prefix("connection error", p.connErr)
	}

	return p.client.HasBlob(hash)
}

// Get downloads the blob from the peer
func (p *Store) Get(hash string) (stream.Blob, error) {
	if p.connErr != nil {
		return nil, errors.Prefix("connection error", p.connErr)
	}

	return p.client.GetBlob(hash)
}

// Put is not supported
func (p *Store) Put(hash string, blob stream.Blob) error {
	panic("PeerStore cannot put or delete blobs")
}

// PutSD is not supported
func (p *Store) PutSD(hash string, blob stream.Blob) error {
	panic("PeerStore cannot put or delete blobs")
}

// Delete is not supported
func (p *Store) Delete(hash string) error {
	panic("PeerStore cannot put or delete blobs")
}
