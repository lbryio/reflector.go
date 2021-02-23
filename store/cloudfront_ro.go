package store

import (
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/shared"

	log "github.com/sirupsen/logrus"
)

// CloudFrontROStore reads from cloudfront. All writes panic.
type CloudFrontROStore struct {
	endpoint string // cloudflare endpoint
}

// NewCloudFrontROStore returns an initialized CloudFrontROStore store pointer.
func NewCloudFrontROStore(endpoint string) *CloudFrontROStore {
	return &CloudFrontROStore{endpoint: endpoint}
}

const nameCloudFrontRO = "cloudfront_ro"

// Name is the cache type name
func (c *CloudFrontROStore) Name() string { return nameCloudFrontRO }

// Has checks if the hash is in the store.
func (c *CloudFrontROStore) Has(hash string) (bool, error) {
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
func (c *CloudFrontROStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	log.Debugf("Getting %s from S3", hash[:8])
	start := time.Now()
	defer func(t time.Time) {
		log.Debugf("Getting %s from S3 took %s", hash[:8], time.Since(t).String())
	}(start)

	status, body, err := c.cfRequest(http.MethodGet, hash)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), c.Name()), err
	}
	defer body.Close()
	switch status {
	case http.StatusNotFound, http.StatusForbidden:
		return nil, shared.NewBlobTrace(time.Since(start), c.Name()), errors.Err(ErrBlobNotFound)
	case http.StatusOK:
		b, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, shared.NewBlobTrace(time.Since(start), c.Name()), errors.Err(err)
		}
		metrics.MtrInBytesS3.Add(float64(len(b)))
		return b, shared.NewBlobTrace(time.Since(start), c.Name()), nil
	default:
		return nil, shared.NewBlobTrace(time.Since(start), c.Name()), errors.Err("unexpected status %d", status)
	}
}

func (c *CloudFrontROStore) cfRequest(method, hash string) (int, io.ReadCloser, error) {
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

func (c *CloudFrontROStore) Put(_ string, _ stream.Blob) error {
	return errors.Err(ErrNotImplemented)
}

func (c *CloudFrontROStore) PutSD(_ string, _ stream.Blob) error {
	return errors.Err(ErrNotImplemented)
}

func (c *CloudFrontROStore) Delete(_ string) error {
	return errors.Err(ErrNotImplemented)
}

// Shutdown shuts down the store gracefully
func (c *CloudFrontROStore) Shutdown() {
	return
}
