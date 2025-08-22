package store

import (
	"io"
	"net/http"
	"path"
	"strconv"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// HttpStore reads from an HTTP endpoint that simply expects the hash to be appended to the endpoint
type HttpStore struct {
	endpoint     string
	httpClient   *http.Client
	prefixLength int
	name         string
}

type HttpParams struct {
	Name         string `mapstructure:"name"`
	Endpoint     string `mapstructure:"endpoint"`
	ShardingSize int    `mapstructure:"sharding_size"`
}

// NewHttpStore returns an initialized HttpStore store pointer.
func NewHttpStore(params HttpParams) *HttpStore {
	return &HttpStore{
		endpoint:     params.Endpoint,
		httpClient:   getClient(),
		prefixLength: params.ShardingSize,
		name:         params.Name,
	}
}

const nameHttp = "http"

func HttpStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg HttpParams
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewHttpStore(cfg), nil
}

// Name is the cache type name
func (c *HttpStore) Name() string { return nameHttp + "-" + c.name }

// Has checks if the hash is in the store.
func (c *HttpStore) Has(hash string) (bool, error) {
	status, body, err := c.cfRequest(http.MethodHead, hash)
	if err != nil {
		return false, err
	}
	defer func() { _ = body.Close() }()
	switch status {
	case http.StatusNotFound, http.StatusForbidden:
		return false, nil
	case http.StatusOK:
		return true, nil
	default:
		return false, errors.Err("unexpected status %d", status)
	}
}

// Get downloads the blob using the http client
func (c *HttpStore) Get(hash string) (b stream.Blob, trace shared.BlobTrace, err error) {
	log.Debugf("Getting %s from HTTP(s) source", hash[:8])
	start := time.Now()

	defer func(t time.Time) {
		log.Debugf("Getting %s from HTTP(s) source took %s", hash[:8], time.Since(t).String())
		trace = trace.Stack(time.Since(start), c.Name())
	}(start)

	url := c.endpoint + c.shardedPath(hash)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, trace, errors.Err(err)
	}
	req.Header.Add("User-Agent", "reflector.go/"+meta.Version())

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, trace, errors.Err(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Errorf("Error closing response body in HTTP-GET: %s", err.Error())
		}
	}(res.Body)

	// Parse Via header if present
	viaHeader := res.Header.Get("Via")
	if viaHeader != "" {
		parsedTrace, err := shared.Deserialize(viaHeader)
		if err == nil {
			trace = *parsedTrace
		}
	}

	switch res.StatusCode {
	case http.StatusNotFound:
		return nil, trace, ErrBlobNotFound
	case http.StatusOK:
		contentLength := res.Header.Get("Content-Length")
		if contentLength != "" {
			size, err := strconv.Atoi(contentLength)
			if err == nil && size > 0 && size <= stream.MaxBlobSize {
				blob := make([]byte, size)
				_, err = io.ReadFull(res.Body, blob)
				if err == nil {
					metrics.MtrInBytesHttp.Add(float64(size))
					return blob, trace, nil
				}
				log.Warnf("Error reading body with known size: %s", err.Error())
			}
		}

		buffer := getBuffer()
		defer putBuffer(buffer)
		if _, err := io.Copy(buffer, res.Body); err != nil {
			return nil, trace, errors.Err(err)
		}
		blob := make([]byte, buffer.Len())
		copy(blob, buffer.Bytes())
		metrics.MtrInBytesHttp.Add(float64(len(blob)))
		return blob, trace, nil
	default:
		body, _ := io.ReadAll(res.Body)
		log.Warnf("Got status code %d (%s)", res.StatusCode, string(body))
		return nil, trace, errors.Err("upstream error. Status code: %d (%s)", res.StatusCode, string(body))
	}
}

func (c *HttpStore) cfRequest(method, hash string) (int, io.ReadCloser, error) {
	url := c.endpoint + c.shardedPath(hash)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return 0, nil, errors.Err(err)
	}
	req.Header.Add("User-Agent", "reflector.go/"+meta.Version())

	res, err := c.httpClient.Do(req)
	if err != nil {
		return 0, nil, errors.Err(err)
	}

	return res.StatusCode, res.Body, nil
}

func (c *HttpStore) Put(_ string, _ stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

func (c *HttpStore) PutSD(_ string, _ stream.Blob) error {
	return errors.Err(shared.ErrNotImplemented)
}

func (c *HttpStore) Delete(_ string) error {
	return errors.Err(shared.ErrNotImplemented)
}

// Shutdown shuts down the store gracefully
func (c *HttpStore) Shutdown() {
}

func (c *HttpStore) shardedPath(hash string) string {
	if c.prefixLength <= 0 || len(hash) < c.prefixLength {
		return hash
	}
	return path.Join(hash[:c.prefixLength], hash)
}

func init() {
	RegisterStore(nameHttp, HttpStoreFactory)
}
