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
	readerStore BlobStore
	writerStore BlobStore
	name        string
}

type ProxiedS3Params struct {
	Name   string    `mapstructure:"name"`
	Reader BlobStore `mapstructure:"reader"`
	Writer BlobStore `mapstructure:"writer"`
}

type ProxiedS3Config struct {
	Name   string `mapstructure:"name"`
	Reader *viper.Viper
	Writer *viper.Viper
}

// NewProxiedS3Store returns an initialized ProxiedS3Store store pointer.
// NOTE: It panics if either argument is nil.
func NewProxiedS3Store(params ProxiedS3Params) *ProxiedS3Store {
	if params.Reader == nil || params.Writer == nil {
		panic("both stores must be set")
	}
	return &ProxiedS3Store{
		readerStore: params.Reader,
		writerStore: params.Writer,
		name:        params.Name,
	}
}

const nameProxiedS3 = "proxied-s3"

// Name is the cache type name
func (c *ProxiedS3Store) Name() string { return nameProxiedS3 + "-" + c.name }

// Has checks if the hash is in the store.
func (c *ProxiedS3Store) Has(hash string) (bool, error) {
	return c.writerStore.Has(hash)
}

// Get gets the blob from Cloudfront.
func (c *ProxiedS3Store) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	blob, trace, err := c.readerStore.Get(hash)
	return blob, trace.Stack(time.Since(start), c.Name()), err
}

// Put stores the blob on S3
func (c *ProxiedS3Store) Put(hash string, blob stream.Blob) error {
	return c.writerStore.Put(hash, blob)
}

// PutSD stores the sd blob on S3
func (c *ProxiedS3Store) PutSD(hash string, blob stream.Blob) error {
	return c.writerStore.PutSD(hash, blob)
}

// Delete deletes the blob from S3
func (c *ProxiedS3Store) Delete(hash string) error {
	return c.writerStore.Delete(hash)
}

// Shutdown shuts down the store gracefully
func (c *ProxiedS3Store) Shutdown() {
	c.writerStore.Shutdown()
	c.readerStore.Shutdown()
}

func ProxiedS3StoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg ProxiedS3Config
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	cfg.Reader = config.Sub("reader")
	cfg.Writer = config.Sub("writer")

	readerStoreType := strings.Split(cfg.Reader.AllKeys()[0], ".")[0]
	readerStoreConfig := cfg.Reader.Sub(readerStoreType)
	factory, ok := Factories[readerStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", readerStoreType)
	}
	readerStore, err := factory(readerStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	writerStoreType := strings.Split(cfg.Writer.AllKeys()[0], ".")[0]
	writerStoreConfig := cfg.Writer.Sub(writerStoreType)
	factory, ok = Factories[writerStoreType]
	if !ok {
		return nil, errors.Err("unknown store type %s", writerStoreType)
	}
	writerStore, err := factory(writerStoreConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	return NewProxiedS3Store(ProxiedS3Params{
		Name:   cfg.Name,
		Reader: readerStore,
		Writer: writerStore,
	}), nil
}

func init() {
	RegisterStore(nameProxiedS3, ProxiedS3StoreFactory)
}
