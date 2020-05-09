package store

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

// DiskBlobStore stores blobs on a local disk
type DiskBlobStore struct {
	// the location of blobs on disk
	blobDir string
	// store files in subdirectories based on the first N chars in the filename. 0 = don't create subdirectories.
	prefixLength int

	initialized     bool
	lastChecked     time.Time
	diskCleanupBusy chan bool
}

// NewDiskBlobStore returns an initialized file disk store pointer.
func NewDiskBlobStore(dir string, prefixLength int) *DiskBlobStore {
	dbs := DiskBlobStore{blobDir: dir, prefixLength: prefixLength, diskCleanupBusy: make(chan bool, 1)}
	dbs.diskCleanupBusy <- true
	return &dbs
}

func (d *DiskBlobStore) dir(hash string) string {
	if d.prefixLength <= 0 || len(hash) < d.prefixLength {
		return d.blobDir
	}
	return path.Join(d.blobDir, hash[:d.prefixLength])
}

// GetUsedSpace returns a value between 0 and 1, with 0 being completely empty and 1 being full, for the disk that holds the provided path
func (d *DiskBlobStore) getUsedSpace() (float32, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(d.blobDir, &stat)
	if err != nil {
		return 0, err
	}
	// Available blocks * size per block = available space in bytes
	all := stat.Blocks * uint64(stat.Bsize)
	free := stat.Bfree * uint64(stat.Bsize)
	used := all - free

	return float32(used) / float32(all), nil
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
	select {
	case <-d.diskCleanupBusy:
		if time.Since(d.lastChecked) > 5*time.Minute {
			go d.ensureDiskSpace()
		}
	default:
		break
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

func (d *DiskBlobStore) ensureDiskSpace() {
	defer func() {
		d.lastChecked = time.Now()
		d.diskCleanupBusy <- true
	}()

	used, err := d.getUsedSpace()
	if err != nil {
		log.Errorln(err.Error())
		return
	}
	log.Infof("disk usage: %.2f%%\n", used*100)
	if used > 0.90 {
		log.Infoln("over 0.90, cleaning up")
		err = d.WipeOldestBlobs()
		if err != nil {
			log.Errorln(err.Error())
			return
		}
		log.Infoln("Done cleaning up")
	}
}

func (d *DiskBlobStore) WipeOldestBlobs() (err error) {
	dirs, err := ioutil.ReadDir(d.blobDir)
	if err != nil {
		return err
	}
	type datedFile struct {
		Atime    time.Time
		File     *os.FileInfo
		FullPath string
	}
	datedFiles := make([]datedFile, 0, 5000)
	for _, dir := range dirs {
		if dir.IsDir() {
			files, err := ioutil.ReadDir(filepath.Join(d.blobDir, dir.Name()))
			if err != nil {
				return err
			}
			for _, file := range files {
				if file.Mode().IsRegular() && !file.IsDir() {
					datedFiles = append(datedFiles, datedFile{
						Atime:    atime(file),
						File:     &file,
						FullPath: filepath.Join(d.blobDir, dir.Name(), file.Name()),
					})
				}
			}
		}
	}

	sort.Slice(datedFiles, func(i, j int) bool {
		return datedFiles[i].Atime.Before(datedFiles[j].Atime)
	})
	//delete the first 50000 blobs
	for i, df := range datedFiles {
		if i >= 50000 {
			break
		}
		log.Infoln(df.FullPath)
		log.Infoln(df.Atime.String())
		err = os.Remove(df.FullPath)
		if err != nil {
			return err
		}
	}
	return nil
}
