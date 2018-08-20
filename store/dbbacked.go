package store

import (
	"encoding/json"
	"sync"

	"github.com/lbryio/reflector.go/db"

	"github.com/lbryio/lbry.go/errors"
	log "github.com/sirupsen/logrus"
)

// DBBackedS3Store is an instance of an S3 Store that is backed by a DB for what is stored.
type DBBackedS3Store struct {
	s3        *S3BlobStore
	db        *db.SQL
	blockedMu sync.RWMutex
	blocked   map[string]bool
}

// NewDBBackedS3Store returns an initialized store pointer.
func NewDBBackedS3Store(s3 *S3BlobStore, db *db.SQL) *DBBackedS3Store {
	return &DBBackedS3Store{s3: s3, db: db}
}

// Has returns true if the blob is in the store
func (d *DBBackedS3Store) Has(hash string) (bool, error) {
	return d.db.HasBlob(hash)
}

// Get gets the blob
func (d *DBBackedS3Store) Get(hash string) ([]byte, error) {
	return d.s3.Get(hash)
}

// Put stores the blob in the S3 store and stores the blob information in the DB.
func (d *DBBackedS3Store) Put(hash string, blob []byte) error {
	err := d.s3.Put(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddBlob(hash, len(blob), true)
}

// PutSD stores the SDBlob in the S3 store. It will return an error if the sd blob is missing the stream hash or if
// there is an error storing the blob information in the DB.
func (d *DBBackedS3Store) PutSD(hash string, blob []byte) error {
	var blobContents db.SdBlob
	err := json.Unmarshal(blob, &blobContents)
	if err != nil {
		return err
	}
	if blobContents.StreamHash == "" {
		return errors.Err("sd blob is missing stream hash")
	}

	err = d.s3.PutSD(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddSDBlob(hash, len(blob), blobContents)
}

func (d *DBBackedS3Store) Delete(hash string) error {
	err := d.s3.Delete(hash)
	if err != nil {
		return err
	}

	return d.db.Delete(hash)
}

// Block deletes the blob and prevents it from being uploaded in the future
func (d *DBBackedS3Store) Block(hash string) error {
	if blocked, err := d.isBlocked(hash); blocked || err != nil {
		return err
	}

	log.Debugf("blocking %s", hash)

	err := d.db.Block(hash)
	if err != nil {
		return err
	}

	has, err := d.db.HasBlob(hash)
	if err != nil {
		return err
	}

	if has {
		err = d.s3.Delete(hash)
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
func (d *DBBackedS3Store) Wants(hash string) (bool, error) {
	has, err := d.Has(hash)
	if has || err != nil {
		return false, err
	}

	blocked, err := d.isBlocked(hash)
	return !blocked, err
}

// MissingBlobsForKnownStream returns missing blobs for an existing stream
// WARNING: if the stream does NOT exist, no blob hashes will be returned, which looks
// like no blobs are missing
func (d *DBBackedS3Store) MissingBlobsForKnownStream(sdHash string) ([]string, error) {
	return d.db.MissingBlobsForKnownStream(sdHash)
}

func (d *DBBackedS3Store) markBlocked(hash string) error {
	err := d.initBlocked()
	if err != nil {
		return err
	}

	d.blockedMu.Lock()
	defer d.blockedMu.Unlock()

	d.blocked[hash] = true
	return nil
}

func (d *DBBackedS3Store) isBlocked(hash string) (bool, error) {
	err := d.initBlocked()
	if err != nil {
		return false, err
	}

	d.blockedMu.RLock()
	defer d.blockedMu.RUnlock()

	return d.blocked[hash], nil
}

func (d *DBBackedS3Store) initBlocked() error {
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
