package store

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/spf13/afero"

	lru "github.com/hashicorp/golang-lru"
)

// DiskBlobStore stores blobs on a local disk
type DiskBlobStore struct {
	// the location of blobs on disk
	blobDir string
	// max number of blobs to store
	maxBlobs int
	// store files in subdirectories based on the first N chars in the filename. 0 = don't create subdirectories.
	prefixLength int

	// lru cache
	lru *lru.Cache
	// filesystem abstraction
	fs afero.Fs

	// true if initOnce ran, false otherwise
	initialized bool
}

// NewDiskBlobStore returns an initialized file disk store pointer.
func NewDiskBlobStore(dir string, maxBlobs, prefixLength int) *DiskBlobStore {
	dbs := DiskBlobStore{
		blobDir:      dir,
		maxBlobs:     maxBlobs,
		prefixLength: prefixLength,
		fs:           afero.NewOsFs(),
	}
	return &dbs
}

func (d *DiskBlobStore) dir(hash string) string {
	if d.prefixLength <= 0 || len(hash) < d.prefixLength {
		return d.blobDir
	}
	return path.Join(d.blobDir, hash[:d.prefixLength])
}

func (d *DiskBlobStore) path(hash string) string {
	return path.Join(d.dir(hash), hash)
}

func (d *DiskBlobStore) ensureDirExists(dir string) error {
	return errors.Err(d.fs.MkdirAll(dir, 0755))
}

func (d *DiskBlobStore) initOnce() error {
	if d.initialized {
		return nil
	}

	err := d.ensureDirExists(d.blobDir)
	if err != nil {
		return err
	}

	l, err := lru.NewWithEvict(d.maxBlobs, func(key interface{}, value interface{}) {
		_ = d.fs.Remove(d.path(key.(string))) // TODO: log this error. may happen if file is gone but cache entry still there?
	})
	if err != nil {
		return errors.Err(err)
	}
	d.lru = l

	err = d.loadExisting()
	if err != nil {
		return err
	}

	d.initialized = true
	return nil
}

// Has returns T/F or Error if it the blob stored already. It will error with any IO disk error.
func (d *DiskBlobStore) Has(hash string) (bool, error) {
	err := d.initOnce()
	if err != nil {
		return false, err
	}

	return d.lru.Contains(hash), nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (d *DiskBlobStore) Get(hash string) (stream.Blob, error) {
	err := d.initOnce()
	if err != nil {
		return nil, err
	}

	_, has := d.lru.Get(hash)
	if !has {
		return nil, errors.Err(ErrBlobNotFound)
	}

	file, err := d.fs.Open(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			d.lru.Remove(hash)
			return nil, errors.Err(ErrBlobNotFound)
		}
		return nil, err
	}
	defer file.Close()

	return ioutil.ReadAll(file)
}

// Put stores the blob on disk
func (d *DiskBlobStore) Put(hash string, blob stream.Blob) error {
	err := d.initOnce()
	if err != nil {
		return err
	}

	err = d.ensureDirExists(d.dir(hash))
	if err != nil {
		return err
	}

	err = afero.WriteFile(d.fs, d.path(hash), blob, 0644)
	if err != nil {
		return errors.Err(err)
	}

	d.lru.Add(hash, true)

	return nil
}

// PutSD stores the sd blob on the disk
func (d *DiskBlobStore) PutSD(hash string, blob stream.Blob) error {
	return d.Put(hash, blob)
}

// Delete deletes the blob from the store
func (d *DiskBlobStore) Delete(hash string) error {
	err := d.initOnce()
	if err != nil {
		return err
	}

	d.lru.Remove(hash)
	return nil
}

// loadExisting scans the blobDir and imports existing blobs into lru cache
func (d *DiskBlobStore) loadExisting() error {
	dirs, err := afero.ReadDir(d.fs, d.blobDir)
	if err != nil {
		return err
	}

	for _, dir := range dirs {
		if dir.IsDir() {
			files, err := afero.ReadDir(d.fs, filepath.Join(d.blobDir, dir.Name()))
			if err != nil {
				return err
			}
			for _, file := range files {
				if file.Mode().IsRegular() && !file.IsDir() {
					d.lru.Add(file.Name(), true)
				}
			}
		}
	}

	return nil
}
