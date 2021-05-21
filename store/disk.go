package store

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/locks"
	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store/speedwalk"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
)

func init() {
	writeCh = make(chan writeRequest)
	for i := 0; i < runtime.NumCPU(); i++ {
		go func() {
			select {
			case r := <-writeCh:
				err := ioutil.WriteFile(r.filename, r.data, r.perm)
				if err != nil {
					log.Errorf("could not write file %s to disk, failed with error: %s", r.filename, err.Error())
				}
			}
		}()
	}
}

var writeCh chan writeRequest

// DiskStore stores blobs on a local disk
type DiskStore struct {
	// the location of blobs on disk
	blobDir string
	// store files in subdirectories based on the first N chars in the filename. 0 = don't create subdirectories.
	prefixLength int

	// true if initOnce ran, false otherwise
	initialized bool

	concurrentChecks atomic.Int32
	lock             locks.MultipleLock
}

const maxConcurrentChecks = 3

// NewDiskStore returns an initialized file disk store pointer.
func NewDiskStore(dir string, prefixLength int) *DiskStore {
	return &DiskStore{
		blobDir:      dir,
		prefixLength: prefixLength,
		lock:         locks.NewMultipleLock(),
	}
}

const nameDisk = "disk"

// Name is the cache type name
func (d *DiskStore) Name() string { return nameDisk }

// Has returns T/F or Error if it the blob stored already. It will error with any IO disk error.
func (d *DiskStore) Has(hash string) (bool, error) {
	d.lock.RLock(hash)
	defer d.lock.RUnlock(hash)
	err := d.initOnce()
	if err != nil {
		return false, err
	}

	_, err = os.Stat(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, errors.Err(err)
	}
	return true, nil
}

// Get returns the blob or an error if the blob doesn't exist.
func (d *DiskStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	d.lock.RLock(hash)
	defer d.lock.RUnlock(hash)
	start := time.Now()
	err := d.initOnce()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), err
	}

	blob, err := ioutil.ReadFile(d.path(hash))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(ErrBlobNotFound)
		}
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(err)
	}

	// this is a rather poor yet effective way of throttling how many blobs can be checked concurrently
	// poor because there is a possible race condition between the check and the actual +1
	if d.concurrentChecks.Load() < maxConcurrentChecks {
		d.concurrentChecks.Add(1)
		defer d.concurrentChecks.Sub(1)
		hashBytes := sha512.Sum384(blob)
		readHash := hex.EncodeToString(hashBytes[:])
		if hash != readHash {
			message := fmt.Sprintf("[%s] found a broken blob while reading from disk. Actual hash: %s", hash, readHash)
			log.Errorf("%s", message)
			err := d.Delete(hash)
			if err != nil {
				return nil, shared.NewBlobTrace(time.Since(start), d.Name()), err
			}
			return nil, shared.NewBlobTrace(time.Since(start), d.Name()), errors.Err(message)
		}
	}

	return blob, shared.NewBlobTrace(time.Since(start), d.Name()), nil
}

// Put stores the blob on disk
func (d *DiskStore) Put(hash string, blob stream.Blob) error {
	d.lock.Lock(hash)
	defer d.lock.Unlock(hash)
	start := time.Now()
	defer func() {
		if time.Since(start) > 100*time.Millisecond {
			log.Infof("it took %s to write %s", time.Since(start), hash)
		}
	}()
	err := d.initOnce()
	if err != nil {
		return err
	}

	err = d.ensureDirExists(d.dir(hash))
	if err != nil {
		return err
	}
	hashBytes := sha512.Sum384(blob)
	readHash := hex.EncodeToString(hashBytes[:])
	matchesBeforeWriting := readHash == hash
	writeFile(d.path(hash), blob, 0644)
	readBlob, err := ioutil.ReadFile(d.path(hash))
	matchesAfterReading := false
	if err != nil {
		log.Errorf("for some fucking reasons I can't read the blob I just wrote %s", err.Error())
	} else {
		hashBytes = sha512.Sum384(readBlob)
		readHash = hex.EncodeToString(hashBytes[:])
		matchesAfterReading = readHash == hash
	}

	log.Infof(`writing %s to disk: hash match: %t, error: %t
reading after writing: hash match: %t`, hash, matchesBeforeWriting, err == nil, matchesAfterReading)

	return errors.Err(err)
}

// PutSD stores the sd blob on the disk
func (d *DiskStore) PutSD(hash string, blob stream.Blob) error {
	d.lock.Lock(hash)
	defer d.lock.Unlock(hash)
	return d.Put(hash, blob)
}

// Delete deletes the blob from the store
func (d *DiskStore) Delete(hash string) error {
	d.lock.Lock(hash)
	defer d.lock.Unlock(hash)
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

	err = os.Remove(d.path(hash))
	return errors.Err(err)
}

// list returns the hashes of blobs that already exist in the blobDir
func (d *DiskStore) list() ([]string, error) {
	err := d.initOnce()
	if err != nil {
		return nil, err
	}

	return speedwalk.AllFiles(d.blobDir, true)
}

func (d *DiskStore) dir(hash string) string {
	if d.prefixLength <= 0 || len(hash) < d.prefixLength {
		return d.blobDir
	}
	return path.Join(d.blobDir, hash[:d.prefixLength])
}

func (d *DiskStore) path(hash string) string {
	return path.Join(d.dir(hash), hash)
}

func (d *DiskStore) ensureDirExists(dir string) error {
	return errors.Err(os.MkdirAll(dir, 0755))
}

func (d *DiskStore) initOnce() error {
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

type writeRequest struct {
	filename string
	data     []byte
	perm     os.FileMode
}

// Shutdown shuts down the store gracefully
func (d *DiskStore) Shutdown() {
	return
}

func writeFile(filename string, data []byte, perm os.FileMode) {
	writeCh <- writeRequest{
		filename: filename,
		data:     data,
		perm:     perm,
	}
}
