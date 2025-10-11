package store

import (
	"strings"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/spf13/viper"
)

// Http3Store is a blob store that gets blobs from a peer over HTTP3.
// It satisfies the BlobStore interface but cannot put or delete blobs.
type Http3Store struct {
	NotFoundCache *sync.Map
	client        *Http3Client
	name          string
	address       string
	timeout       time.Duration
	clientMu      sync.RWMutex
}

// Http3Params allows to set options for a new Http3Store.
type Http3Params struct {
	Name    string        `mapstructure:"name"`
	Address string        `mapstructure:"address"`
	Timeout time.Duration `mapstructure:"timeout"`
}

// NewHttp3Store makes a new HTTP3 store.
func NewHttp3Store(params Http3Params) *Http3Store {
	return &Http3Store{
		name:          params.Name,
		NotFoundCache: &sync.Map{},
		address:       params.Address,
		timeout:       params.Timeout,
	}
}

const nameHttp3 = "http3"

func Http3StoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg Http3Params
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewHttp3Store(cfg), nil
}

func init() {
	RegisterStore(nameHttp3, Http3StoreFactory)
}

func (h *Http3Store) Name() string { return nameHttp3 + "-" + h.name }

func (h *Http3Store) getClient() (*Http3Client, error) {
	h.clientMu.RLock()
	if h.client != nil {
		client := h.client
		h.clientMu.RUnlock()
		return client, nil
	}
	h.clientMu.RUnlock()

	h.clientMu.Lock()
	defer h.clientMu.Unlock()

	// Check again in case another goroutine created the client
	if h.client != nil {
		return h.client, nil
	}

	client, err := NewHttp3Client(h.address)
	if err != nil {
		return nil, err
	}
	h.client = client
	return client, nil
}

// Has asks the peer if they have a hash
func (h *Http3Store) Has(hash string) (bool, error) {
	c, err := h.getClient()
	if err != nil {
		return false, err
	}
	return c.HasBlob(hash)
}

// Get downloads the blob from the peer
func (h *Http3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	if lastChecked, ok := h.NotFoundCache.Load(hash); ok {
		if lastChecked.(time.Time).After(time.Now().Add(-5 * time.Minute)) {
			return nil, shared.NewBlobTrace(time.Since(start), h.Name()+"-notfoundcache"), ErrBlobNotFound
		}
	}
	c, err := h.getClient()
	if err != nil && strings.Contains(err.Error(), "blob not found") {
		h.NotFoundCache.Store(hash, time.Now())
	}
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), h.Name()), err
	}
	return c.GetBlob(hash)
}

// Put is not supported
func (h *Http3Store) Put(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// PutSD is not supported
func (h *Http3Store) PutSD(hash string, blob stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Delete is not supported
func (h *Http3Store) Delete(hash string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown shuts down the store gracefully
func (h *Http3Store) Shutdown() {
	h.clientMu.Lock()
	defer h.clientMu.Unlock()
	if h.client != nil {
		_ = h.client.Close()
		h.client = nil
	}
}
