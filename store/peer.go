package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/spf13/viper"
)

// PeerStore is a blob store that gets blobs from a peer.
// It satisfies the BlobStore interface but cannot put or delete blobs.
type PeerStore struct {
	name string
	opts PeerParams
}

// PeerParams allows to set options for a new PeerStore.
type PeerParams struct {
	Name    string        `mapstructure:"name"`
	Address string        `mapstructure:"address"`
	Timeout time.Duration `mapstructure:"timeout"`
}

// NewPeerStore makes a new peer store.
func NewPeerStore(params PeerParams) *PeerStore {
	return &PeerStore{opts: params, name: params.Name}
}

const namePeer = "peer"

func PeerStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg PeerParams
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewPeerStore(cfg), nil
}

func init() {
	RegisterStore(namePeer, PeerStoreFactory)
}

func (p *PeerStore) Name() string { return namePeer + "-" + p.name }

func (p *PeerStore) getClient() (*PeerClient, error) {
	c := &PeerClient{Timeout: p.opts.Timeout}
	err := c.Connect(p.opts.Address)
	return c, errors.Prefix("connection error", err)
}

// Has asks the peer if they have a hash
func (p *PeerStore) Has(hash string) (bool, error) {
	c, err := p.getClient()
	if err != nil {
		return false, err
	}
	defer func() { _ = c.Close() }()
	return c.HasBlob(hash)
}

// Get downloads the blob from the peer
func (p *PeerStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	c, err := p.getClient()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), p.Name()), err
	}
	defer func() { _ = c.Close() }()
	blob, trace, err := c.GetBlob(hash)
	if err != nil && strings.Contains(err.Error(), "blob not found") {
		return nil, trace, ErrBlobNotFound
	}

	return blob, trace, err
}

// Put is not supported
func (p *PeerStore) Put(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// PutSD is not supported
func (p *PeerStore) PutSD(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Delete is not supported
func (p *PeerStore) Delete(hash string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown is not supported
func (p *PeerStore) Shutdown() {
}
