//go:build !linux && !darwin
// +build !linux,!darwin

package store

import (
	"os"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
)

// Put stores the blob on disk
func (d *DiskStore) Put(hash string, blob stream.Blob) error {
	err := d.initOnce()
	if err != nil {
		return err
	}

	err = d.ensureDirExists(d.dir(hash))
	if err != nil {
		return err
	}

	err = os.WriteFile(d.tmpPath(hash), blob, 0644)
	if err != nil {
		return errors.Err(err)
	}
	err = os.Rename(d.tmpPath(hash), d.path(hash))
	return errors.Err(err)
}
