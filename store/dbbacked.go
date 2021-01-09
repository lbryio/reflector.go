package store

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/shared"

	log "github.com/sirupsen/logrus"
)

// DBBackedStore is a store that's backed by a DB. The DB contains data about what's in the store.
type DBBackedStore struct {
	blobs        BlobStore
	db           *db.SQL
	blockedMu    sync.RWMutex
	blocked      map[string]bool
	deleteOnMiss bool
}

// NewDBBackedStore returns an initialized store pointer.
func NewDBBackedStore(blobs BlobStore, db *db.SQL, deleteOnMiss bool) *DBBackedStore {
	return &DBBackedStore{blobs: blobs, db: db, deleteOnMiss: deleteOnMiss}
}

const nameDBBacked = "db-backed"

// Name is the cache type name
func (d *DBBackedStore) Name() string { return nameDBBacked }

// Has returns true if the blob is in the store
func (d *DBBackedStore) Has(hash string) (bool, error) {
	return d.db.HasBlob(hash, false)
}

// Get gets the blob
func (d *DBBackedStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	has, err := d.db.HasBlob(hash, true)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), err
	}
	if !has {
		return nil, shared.NewBlobTrace(time.Since(start), d.Name()), ErrBlobNotFound
	}

	b, stack, err := d.blobs.Get(hash)
	if d.deleteOnMiss && errors.Is(err, ErrBlobNotFound) {
		e2 := d.Delete(hash)
		if e2 != nil {
			log.Errorf("error while deleting blob from db: %s", errors.FullTrace(err))
		}
	}

	return b, stack.Stack(time.Since(start), d.Name()), err
}

// Put stores the blob in the S3 store and stores the blob information in the DB.
func (d *DBBackedStore) Put(hash string, blob stream.Blob) error {
	err := d.blobs.Put(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddBlob(hash, len(blob), true)
}

// PutSD stores the SDBlob in the S3 store. It will return an error if the sd blob is missing the stream hash or if
// there is an error storing the blob information in the DB.
func (d *DBBackedStore) PutSD(hash string, blob stream.Blob) error {
	var blobContents db.SdBlob
	err := json.Unmarshal(blob, &blobContents)
	if err != nil {
		return errors.Err(err)
	}
	if blobContents.StreamHash == "" {
		return errors.Err("sd blob is missing stream hash")
	}

	err = d.blobs.PutSD(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddSDBlob(hash, len(blob), blobContents)
}

func (d *DBBackedStore) Delete(hash string) error {
	err := d.blobs.Delete(hash)
	if err != nil {
		return err
	}

	return d.db.Delete(hash)
}

// Block deletes the blob and prevents it from being uploaded in the future
func (d *DBBackedStore) Block(hash string) error {
	if blocked, err := d.isBlocked(hash); blocked || err != nil {
		return err
	}

	log.Debugf("blocking %s", hash)

	err := d.db.Block(hash)
	if err != nil {
		return err
	}

	has, err := d.db.HasBlob(hash, false)
	if err != nil {
		return err
	}

	if has {
		err = d.blobs.Delete(hash)
		if err != nil {
			return err
		}

		err = d.db.Delete(hash)
		if err != nil {
			return err
		}
	}

	return d.markBlocked(hash)
}

// Wants returns false if the hash exists or is blocked, true otherwise
func (d *DBBackedStore) Wants(hash string) (bool, error) {
	blocked, err := d.isBlocked(hash)
	if blocked || err != nil {
		return false, err
	}

	has, err := d.Has(hash)
	return !has, err
}

// MissingBlobsForKnownStream returns missing blobs for an existing stream
// WARNING: if the stream does NOT exist, no blob hashes will be returned, which looks
// like no blobs are missing
func (d *DBBackedStore) MissingBlobsForKnownStream(sdHash string) ([]string, error) {
	return d.db.MissingBlobsForKnownStream(sdHash)
}

func (d *DBBackedStore) markBlocked(hash string) error {
	err := d.initBlocked()
	if err != nil {
		return err
	}

	d.blockedMu.Lock()
	defer d.blockedMu.Unlock()

	d.blocked[hash] = true
	return nil
}

func (d *DBBackedStore) isBlocked(hash string) (bool, error) {
	err := d.initBlocked()
	if err != nil {
		return false, err
	}

	d.blockedMu.RLock()
	defer d.blockedMu.RUnlock()

	return d.blocked[hash], nil
}

func (d *DBBackedStore) initBlocked() error {
	// first check without blocking since this is the most likely scenario
	if d.blocked != nil {
		return nil
	}

	d.blockedMu.Lock()
	defer d.blockedMu.Unlock()

	// check again in case of race condition
	if d.blocked != nil {
		return nil
	}

	var err error
	d.blocked, err = d.db.GetBlocked()

	return err
}

// Shutdown shuts down the store gracefully
func (d *DBBackedStore) Shutdown() {
	d.blobs.Shutdown()
	return
}
