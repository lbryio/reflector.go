package store

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/lbryio/errors.go"
)

type FileBlobStore struct {
	dir string

	initialized bool
}

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
	defer func() { f.initialized = true }()

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
	return nil
}

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

func (f *FileBlobStore) Get(hash string) ([]byte, error) {
	err := f.initOnce()
	if err != nil {
		return []byte{}, err
	}

	file, err := os.Open(f.path(hash))
	if err != nil {
		return []byte{}, err
	}

	return ioutil.ReadAll(file)
}

func (f *FileBlobStore) Put(hash string, blob []byte) error {
	err := f.initOnce()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(f.path(hash), blob, 0644)
}

func (f *FileBlobStore) PutSD(hash string, blob []byte) error {
	return f.Put(hash, blob)
}
