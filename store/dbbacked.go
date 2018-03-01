package store

import (
	"encoding/json"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/types"
)

type DBBackedS3Store struct {
	s3 *S3BlobStore
	db db.DB
}

func NewDBBackedS3Store(s3 *S3BlobStore, db db.DB) *DBBackedS3Store {
	return &DBBackedS3Store{s3: s3, db: db}
}

func (d *DBBackedS3Store) Has(hash string) (bool, error) {
	return d.db.HasBlob(hash)
}

func (d *DBBackedS3Store) Get(hash string) ([]byte, error) {
	return d.s3.Get(hash)
}

func (d *DBBackedS3Store) Put(hash string, blob []byte) error {
	err := d.s3.Put(hash, blob)
	if err != nil {
		return err
	}

	return d.db.AddBlob(hash, len(blob), true)
}

func (d *DBBackedS3Store) PutSD(hash string, blob []byte) error {
	var blobContents types.SdBlob
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
