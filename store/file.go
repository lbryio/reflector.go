package store

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/lbryio/lbry.go/errors"
)

// FileBlobStore is a local disk store.
type FileBlobStore struct {
	dir string

	initialized bool
}

// NewFileBlobStore returns an initialized file disk store pointer.
func NewFileBlobStore(dir string) *FileBlobStore {
	return &FileBlobStore{dir: dir}
}

func (f *FileBlobStore) path(hash string) string {
	return path.Join(f.dir, hash)
}

func (f *FileBlobStore) initOnce() error {
	if f.initialized {
		return nil
	}

	if stat, err := os.Stat(f.dir); err != nil {
		if os.IsNotExist(err) {
			err2 := os.Mkdir(f.dir, 0755)
			if err2 != nil {
				return err2
			}
		} else {
			return err
		}
	} else if !stat.IsDir() {
		return errors.Err("blob dir exists but is not a dir")
	}

	f.initialized = true
	return nil
}

// Has returns T/F or Error if it the blob stored already. It will error with any IO disk error.
func (f *FileBlobStore) Has(hash string) (bool, error) {
	err := f.initOnce()
	if err != nil {
		return false, err
	}

	_, err = os.Stat(f.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Get returns the byte slice of the blob stored or will error if the blob doesn't exist.
func (f *FileBlobStore) Get(hash string) ([]byte, error) {
	err := f.initOnce()
	if err != nil {
		return []byte{}, err
	}

	file, err := os.Open(f.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return []byte{}, errors.Err(ErrBlobNotFound)
		}
		return []byte{}, err
	}

	return ioutil.ReadAll(file)
}

// Put stores the blob on disk
func (f *FileBlobStore) Put(hash string, blob []byte) error {
	err := f.initOnce()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(f.path(hash), blob, 0644)
}

// PutSD stores the sd blob on the disk
func (f *FileBlobStore) PutSD(hash string, blob []byte) error {
	return f.Put(hash, blob)
}
