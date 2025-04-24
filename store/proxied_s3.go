package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/spf13/viper"
)

// ProxiedS3Store writes to an S3 store and reads from any BlobStore (usually an ITTTStore of HttpStore endpoints).
type ProxiedS3Store struct {
	proxied BlobStore
	s3      *S3Store
	name    string
}

type ProxiedS3Params struct {
	Name    string    `mapstructure:"name"`
	Proxied BlobStore `mapstructure:"proxied"`
	S3      *S3Store  `mapstructure:"s3"`
}

type ProxiedS3Config struct {
	Name    string `mapstructure:"name"`
	Proxied *viper.Viper
	S3      *viper.Viper
}

// NewProxiedS3Store returns an initialized ProxiedS3Store store pointer.
// NOTE: It panics if either argument is nil.
func NewProxiedS3Store(params ProxiedS3Params) *ProxiedS3Store {
	if params.Proxied == nil || params.S3 == nil {
		panic("both stores must be set")
	}
	return &ProxiedS3Store{
		proxied: params.Proxied,
		s3:      params.S3,
		name:    params.Name,
	}
}

const nameProxiedS3 = "proxied-s3"

// Name is the cache type name
func (c *ProxiedS3Store) Name() string { return nameProxiedS3 + "-" + c.name }

// Has checks if the hash is in the store.
func (c *ProxiedS3Store) Has(hash string) (bool, error) {
	return c.proxied.Has(hash)
}

// Get gets the blob from Cloudfront.
func (c *ProxiedS3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.proxied.Get(hash)
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
	c.proxied.Shutdown()
}

func ProxiedS3StoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg ProxiedS3Config
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	cfg.Proxied = config.Sub("proxied")
	cfg.S3 = config.Sub("s3")

	proxiedStoreType := strings.Split(cfg.Proxied.AllKeys()[0], ".")[0]
	proxiedStoreConfig := cfg.Proxied.Sub(proxiedStoreType)
	factory, ok := Factories[proxiedStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", proxiedStoreType)
	}
	proxiedStore, err := factory(proxiedStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	s3StoreType := strings.Split(cfg.S3.AllKeys()[0], ".")[0]
	s3StoreConfig := cfg.S3.Sub(s3StoreType)
	factory, ok = Factories[s3StoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", s3StoreType)
	}
	s3Store, err := factory(s3StoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	s3StoreTyped, ok := s3Store.(*S3Store)
	if !ok {
		return nil, errors.Err("s3 store must be of type S3Store")
	}

	return NewProxiedS3Store(ProxiedS3Params{
		Name:    cfg.Name,
		Proxied: proxiedStore,
		S3:      s3StoreTyped,
	}), nil
}

func init() {
	RegisterStore(nameProxiedS3, ProxiedS3StoreFactory)
}
