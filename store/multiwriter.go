package store

import (
	"strings"
	"sync"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/shared"
	"github.com/spf13/viper"
)

// MultiWriterStore writes to multiple destination stores
type MultiWriterStore struct {
	name         string
	destinations []BlobStore
}

type MultiWriterParams struct {
	Name         string
	Destinations []BlobStore
}

type MultiWriterConfig struct {
	Name         string   `mapstructure:"name"`
	Destinations []string `mapstructure:"destinations"`
}

// NewMultiWriterStore returns a new instance of the MultiWriter store
func NewMultiWriterStore(params MultiWriterParams) *MultiWriterStore {
	return &MultiWriterStore{
		name:         params.Name,
		destinations: params.Destinations,
	}
}

const nameMultiWriter = "multiwriter"

func MultiWriterStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg MultiWriterConfig
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	var destinations []BlobStore
	for _, destType := range cfg.Destinations {
		destConfig := config.Sub(destType)
		if destConfig == nil {
			return nil, errors.Err("missing configuration for destination %s", destType)
		}

		factory, ok := Factories[destType]
		if !ok {
			return nil, errors.Err("unknown store type %s", destType)
		}

		destStore, err := factory(destConfig)
		if err != nil {
			return nil, errors.Err(err)
		}
		destinations = append(destinations, destStore)
	}

	return NewMultiWriterStore(MultiWriterParams{
		Name:         cfg.Name,
		Destinations: destinations,
	}), nil
}

func init() {
	RegisterStore(nameMultiWriter, MultiWriterStoreFactory)
}

// Name returns the store name
func (m *MultiWriterStore) Name() string { return nameMultiWriter + "-" + m.name }

// Has is not supported by MultiWriter
func (m *MultiWriterStore) Has(hash string) (bool, error) {
	return false, errors.Err(shared.ErrNotImplemented)
}

// Get is not supported by MultiWriter
func (m *MultiWriterStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	return nil, shared.BlobTrace{}, errors.Err(shared.ErrNotImplemented)
}

// Put writes the blob to all destination stores
func (m *MultiWriterStore) Put(hash string, blob stream.Blob) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.destinations))

	for _, dest := range m.destinations {
		wg.Add(1)
		go func(d BlobStore) {
			defer wg.Done()
			if err := d.Put(hash, blob); err != nil {
				errChan <- errors.Err("failed to write to %s: %v", d.Name(), err)
			}
		}(dest)
	}

	wg.Wait()
	close(errChan)

	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return errors.Err("failed to write to some destinations: %s", strings.Join(errs, "; "))
	}
	return nil
}

// PutSD writes the SD blob to all destination stores
func (m *MultiWriterStore) PutSD(hash string, blob stream.Blob) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.destinations))

	for _, dest := range m.destinations {
		wg.Add(1)
		go func(d BlobStore) {
			defer wg.Done()
			if err := d.PutSD(hash, blob); err != nil {
				errChan <- errors.Err("failed to write SD to %s: %v", d.Name(), err)
			}
		}(dest)
	}

	wg.Wait()
	close(errChan)

	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return errors.Err("failed to write SD to some destinations: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Delete deletes the blob from all destination stores
func (m *MultiWriterStore) Delete(hash string) error {
	var wg sync.WaitGroup
	errChan := make(chan error, len(m.destinations))

	for _, dest := range m.destinations {
		wg.Add(1)
		go func(d BlobStore) {
			defer wg.Done()
			if err := d.Delete(hash); err != nil {
				errChan <- errors.Err("failed to delete from %s: %v", d.Name(), err)
			}
		}(dest)
	}

	wg.Wait()
	close(errChan)

	var errs []string
	for err := range errChan {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return errors.Err("failed to delete from some destinations: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Shutdown shuts down all destination stores gracefully
func (m *MultiWriterStore) Shutdown() {
	for _, dest := range m.destinations {
		dest.Shutdown()
	}
}
