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
func (c *CloudFrontROStore) Get(hash string) (stream.Blob, error) {
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
	panic("CloudFrontROStore cannot do writes. Use CloudFrontRWStore")
}

func (c *CloudFrontROStore) PutSD(_ string, _ stream.Blob) error {
	panic("CloudFrontROStore cannot do writes. Use CloudFrontRWStore")
}

func (c *CloudFrontROStore) Delete(_ string) error {
	panic("CloudFrontROStore cannot do writes. Use CloudFrontRWStore")
}

// Shutdown shuts down the store gracefully
func (c *CloudFrontROStore) Shutdown() {
	return
}
