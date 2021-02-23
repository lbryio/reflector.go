package http3

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/shared"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
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
	var qconf quic.Config
	qconf.HandshakeTimeout = 4 * time.Second
	qconf.MaxIdleTimeout = 20 * time.Second
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: true,
		},
		QuicConfig: &qconf,
	}
	connection := &http.Client{
		Transport: roundTripper,
	}
	c := &Client{
		conn:         connection,
		roundTripper: roundTripper,
		ServerAddr:   p.opts.Address,
	}
	return c, errors.Prefix("connection error", err)
}

func (p *Store) Name() string { return "http3" }

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
	return c.GetBlob(hash)
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

// Delete is not supported
func (p *Store) Shutdown() {
	return
}
