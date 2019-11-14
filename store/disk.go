package store

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// DiskBlobStore stores blobs on a local disk
type DiskBlobStore struct {
	// the location of blobs on disk
	blobDir string
	// store files in subdirectories based on the first N chars in the filename. 0 = don't create subdirectories.
	prefixLength int

	initialized bool
}

// NewDiskBlobStore returns an initialized file disk store pointer.
func NewDiskBlobStore(dir string, prefixLength int) *DiskBlobStore {
	return &DiskBlobStore{blobDir: dir, prefixLength: prefixLength}
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
	return errors.Err(os.MkdirAll(dir, 0755))
}

func (d *DiskBlobStore) initOnce() error {
	if d.initialized {
		return nil
	}

	err := d.ensureDirExists(d.blobDir)
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

	_, err = os.Stat(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (d *DiskBlobStore) Get(hash string) (stream.Blob, error) {
	err := d.initOnce()
	if err != nil {
		return nil, err
	}

	file, err := os.Open(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Err(ErrBlobNotFound)
		}
		return nil, err
	}

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

	return ioutil.WriteFile(d.path(hash), blob, 0644)
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

	has, err := d.Has(hash)
	if err != nil {
		return err
	}
	if !has {
		return nil
	}

	return os.Remove(d.path(hash))
}
