package store

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/meta"

	log "github.com/sirupsen/logrus"
)

// CloudFrontBlobStore is an CloudFront backed store (retrieval only)
type CloudFrontBlobStore struct {
	cfEndpoint string
	s3Store    *S3BlobStore
}

// NewS3BlobStore returns an initialized S3 store pointer.
func NewCloudFrontBlobStore(cloudFrontEndpoint string, S3Store *S3BlobStore) *CloudFrontBlobStore {
	return &CloudFrontBlobStore{
		cfEndpoint: cloudFrontEndpoint,
		s3Store:    S3Store,
	}
}

// Has returns T/F or Error if the store contains the blob.
func (s *CloudFrontBlobStore) Has(hash string) (bool, error) {
	url := s.cfEndpoint + hash

	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, errors.Err(err)
	}
	req.Header.Add("User-Agent", "reflector.go/"+meta.Version)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, errors.Err(err)
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusNotFound, http.StatusForbidden:
		return false, nil
	case http.StatusOK:
		return true, nil
	default:
		return false, errors.Err(res.Status)
	}
}

// Get returns the blob slice if present or errors.
func (s *CloudFrontBlobStore) Get(hash string) (stream.Blob, error) {
	url := s.cfEndpoint + hash
	log.Debugf("Getting %s from S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", hash[:8], time.Since(t).String())
	}(time.Now())
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.Err(err)
	}
	req.Header.Add("User-Agent", "reflector.go/"+meta.Version)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusNotFound, http.StatusForbidden:
		return nil, errors.Err(ErrBlobNotFound)
	case http.StatusOK:
		b, err := ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, errors.Err(err)
		}
		return b, nil
	default:
		return nil, errors.Err(res.Status)
	}
}

// Put stores the blob on S3 or errors if S3 store is not present.
func (s *CloudFrontBlobStore) Put(hash string, blob stream.Blob) error {
	if s.s3Store != nil {
		return s.s3Store.Put(hash, blob)
	}
	return errors.Err("not implemented in cloudfront store")
}

// PutSD stores the sd blob on S3 or errors if S3 store is not present.
func (s *CloudFrontBlobStore) PutSD(hash string, blob stream.Blob) error {
	if s.s3Store != nil {
		return s.s3Store.PutSD(hash, blob)
	}
	return errors.Err("not implemented in cloudfront store")
}

func (s *CloudFrontBlobStore) Delete(hash string) error {
	if s.s3Store != nil {
		return s.s3Store.Delete(hash)
	}
	return errors.Err("not implemented in cloudfront store")
}
