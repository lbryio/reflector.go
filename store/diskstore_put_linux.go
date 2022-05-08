//go:build linux
// +build linux

package store

import (
	"bytes"
	"io"
	"os"
	"syscall"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/brk0v/directio"
)

var openFileFlags = os.O_WRONLY | os.O_CREATE | syscall.O_DIRECT

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

	// Open file with O_DIRECT
	f, err := os.OpenFile(d.tmpPath(hash), openFileFlags, 0644)
	if err != nil {
		return errors.Err(err)
	}
	defer f.Close()

	dio, err := directio.New(f)
	if err != nil {
		return errors.Err(err)
	}
	defer dio.Flush()

	_, err = io.Copy(dio, bytes.NewReader(blob))
	if err != nil {
		return errors.Err(err)
	}
	err = os.Rename(d.tmpPath(hash), d.path(hash))
	return errors.Err(err)
}
