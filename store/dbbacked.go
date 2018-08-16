package store

import (
	"encoding/json"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/reflector.go/db"
)

// DBBackedS3Store is an instance of an S3 Store that is backed by a DB for what is stored.
type DBBackedS3Store struct {
	s3 *S3BlobStore
	db *db.SQL
}

// NewDBBackedS3Store returns an initialized store pointer.
func NewDBBackedS3Store(s3 *S3BlobStore, db *db.SQL) *DBBackedS3Store {
	return &DBBackedS3Store{s3: s3, db: db}
}

// Has returns T/F or Error ( if the DB errors ) if store contains the blob.
func (d *DBBackedS3Store) Has(hash string) (bool, error) {
	return d.db.HasBlob(hash)
}

// Get returns the byte slice of the blob or an error.
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

// MissingBlobsForKnownStream returns missing blobs for an existing stream
// WARNING: if the stream does NOT exist, no blob hashes will be returned, which looks
// like no blobs are missing
func (d *DBBackedS3Store) MissingBlobsForKnownStream(sdHash string) ([]string, error) {
	return d.db.MissingBlobsForKnownStream(sdHash)
}
