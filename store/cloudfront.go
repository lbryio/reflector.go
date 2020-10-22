package store

import (
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

// CloudFrontStore wraps an S3 store. Reads go to Cloudfront, writes go to S3.
type CloudFrontStore struct {
	s3       *S3Store
	endpoint string // cloudflare endpoint
}

// NewCloudFrontStore returns an initialized CloudFrontStore store pointer.
// NOTE: It panics if S3Store is nil.
func NewCloudFrontStore(s3 *S3Store, cfEndpoint string) *CloudFrontStore {
	if s3 == nil {
		panic("S3Store must not be nil")
	}

	return &CloudFrontStore{
		endpoint: cfEndpoint,
		s3:       s3,
	}
}

const nameCloudFront = "cloudfront"

// Name is the cache type name
func (c *CloudFrontStore) Name() string { return nameCloudFront }

// Has checks if the hash is in the store.
func (c *CloudFrontStore) Has(hash string) (bool, error) {
	status, body, err := c.cfRequest(http.MethodHead, hash)
	if err != nil {
		return false, err
	}
	defer body.Close()

	switch status {
	case http.StatusNotFound, http.StatusForbidden:
		return false, nil
	case http.StatusOK:
		return true, nil
	default:
		return false, errors.Err("unexpected status %d", status)
	}
}

// Get gets the blob from Cloudfront.
func (c *CloudFrontStore) Get(hash string) (stream.Blob, error) {
	log.Debugf("Getting %s from S3", hash[:8])
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", hash[:8], time.Since(t).String())
	}(time.Now())

	status, body, err := c.cfRequest(http.MethodGet, hash)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	switch status {
	case http.StatusNotFound, http.StatusForbidden:
		return nil, errors.Err(ErrBlobNotFound)
	case http.StatusOK:
		b, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, errors.Err(err)
		}
		metrics.MtrInBytesS3.Add(float64(len(b)))
		return b, nil
	default:
		return nil, errors.Err("unexpected status %d", status)
	}
}

func (c *CloudFrontStore) cfRequest(method, hash string) (int, io.ReadCloser, error) {
	url := c.endpoint + hash
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return 0, nil, errors.Err(err)
	}
	req.Header.Add("User-Agent", "reflector.go/"+meta.Version)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, errors.Err(err)
	}

	return res.StatusCode, res.Body, nil
}

// Put stores the blob on S3
func (c *CloudFrontStore) Put(hash string, blob stream.Blob) error {
	return c.s3.Put(hash, blob)
}

// PutSD stores the sd blob on S3
func (c *CloudFrontStore) PutSD(hash string, blob stream.Blob) error {
	return c.s3.PutSD(hash, blob)
}

// Delete deletes the blob from S3
func (c *CloudFrontStore) Delete(hash string) error {
	return c.s3.Delete(hash)
}
