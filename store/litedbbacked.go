package store

import (
	"encoding/json"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/lite_db"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

// DBBackedStore is a store that's backed by a DB. The DB contains data about what's in the store.
type LiteDBBackedStore struct {
	blobs     BlobStore
	db        *lite_db.SQL
	maxItems  int
	stopper   *stop.Stopper
	component string
}

// NewDBBackedStore returns an initialized store pointer.
func NewLiteDBBackedStore(component string, blobs BlobStore, db *lite_db.SQL, maxItems int) *LiteDBBackedStore {
	instance := &LiteDBBackedStore{blobs: blobs, db: db, maxItems: maxItems, stopper: stop.New(), component: component}
	go func() {
		instance.selfClean()
	}()
	return instance
}

const nameLiteDBBacked = "lite-db-backed"

// Name is the cache type name
func (d *LiteDBBackedStore) Name() string { return nameDBBacked }

// Has returns true if the blob is in the store
func (d *LiteDBBackedStore) Has(hash string) (bool, error) {
	return d.db.HasBlob(hash)
}

// Get gets the blob
func (d *LiteDBBackedStore) Get(hash string) (stream.Blob, error) {
	has, err := d.db.HasBlob(hash)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, ErrBlobNotFound
	}
	b, err := d.blobs.Get(hash)
	if err != nil && errors.Is(err, ErrBlobNotFound) {
		e2 := d.db.Delete(hash)
		if e2 != nil {
			log.Errorf("error while deleting blob from db: %s", errors.FullTrace(e2))
		}
		return b, err
	}

	return d.blobs.Get(hash)
}

// Put stores the blob in the S3 store and stores the blob information in the DB.
func (d *LiteDBBackedStore) Put(hash string, blob stream.Blob) error {
	err := d.blobs.Put(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddBlob(hash, len(blob))
}

// PutSD stores the SDBlob in the S3 store. It will return an error if the sd blob is missing the stream hash or if
// there is an error storing the blob information in the DB.
func (d *LiteDBBackedStore) PutSD(hash string, blob stream.Blob) error {
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

	return d.db.AddSDBlob(hash, len(blob))
}

func (d *LiteDBBackedStore) Delete(hash string) error {
	err := d.blobs.Delete(hash)
	if err != nil {
		return err
	}

	return d.db.Delete(hash)
}

// list returns the hashes of blobs that already exist in the database
func (d *LiteDBBackedStore) list() ([]string, error) {
	blobs, err := d.db.AllBlobs()
	return blobs, err
}

func (d *LiteDBBackedStore) selfClean() {
	d.stopper.Add(1)
	defer d.stopper.Done()
	lastCleanup := time.Now()
	const cleanupInterval = 10 * time.Second
	for {
		select {
		case <-d.stopper.Ch():
			log.Infoln("stopping self cleanup")
			return
		default:
			time.Sleep(1 * time.Second)
		}
		if time.Since(lastCleanup) < cleanupInterval {
			continue
		}
		blobsCount, err := d.db.BlobsCount()
		if err != nil {
			log.Errorf(errors.FullTrace(err))
		}
		if blobsCount >= d.maxItems {
			itemsToDelete := blobsCount / 100 * 10
			blobs, err := d.db.GetLRUBlobs(itemsToDelete)
			if err != nil {
				log.Errorf(errors.FullTrace(err))
			}
			for _, hash := range blobs {
				select {
				case <-d.stopper.Ch():
					return
				default:

				}
				err = d.Delete(hash)
				if err != nil {
					log.Errorf(errors.FullTrace(err))
				}
				metrics.CacheLRUEvictCount.With(metrics.CacheLabels(d.Name(), d.component)).Inc()
			}
		}
		lastCleanup = time.Now()
	}
}

// Shutdown shuts down the store gracefully
func (d *LiteDBBackedStore) Shutdown() {
	d.stopper.StopAndWait()
	return
}
