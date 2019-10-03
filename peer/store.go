package peer

import (
	"github.com/lbryio/lbry.go/extras/errors"
)

// Store is a blob store that gets blobs from a peer.
// It satisfies the store.BlobStore interface but cannot put or delete blobs.
type Store struct {
	client  *Client
	connErr error
}

// NewStore makes a new peer store.
func NewStore(clientAddress string) *Store {
	c := &Client{}
	err := c.Connect(clientAddress)
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
func (p *Store) Get(hash string) ([]byte, error) {
	if p.connErr != nil {
		return nil, errors.Prefix("connection error", p.connErr)
	}

	return p.client.GetBlob(hash)
}

// Put is not supported
func (p *Store) Put(hash string, blob []byte) error {
	panic("PeerStore cannot put or delete blobs")
}

// PutSD is not supported
func (p *Store) PutSD(hash string, blob []byte) error {
	panic("PeerStore cannot put or delete blobs")
}

// Delete is not supported
func (p *Store) Delete(hash string) error {
	panic("PeerStore cannot put or delete blobs")
}
