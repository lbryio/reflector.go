package store

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// UpstreamStore is a store that works on top of the HTTP protocol
type UpstreamStore struct {
	upstream   string
	httpClient *http.Client
	edgeToken  string
	name       string
}

type UpstreamParams struct {
	Name      string `mapstructure:"name"`
	Upstream  string `mapstructure:"upstream"`
	EdgeToken string `mapstructure:"edge_token"`
}

func NewUpstreamStore(params UpstreamParams) *UpstreamStore {
	return &UpstreamStore{
		upstream:   params.Upstream,
		httpClient: getClient(),
		edgeToken:  params.EdgeToken,
		name:       params.Name,
	}
}

const nameUpstream = "upstream"

func UpstreamStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg UpstreamParams
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}
	return NewUpstreamStore(cfg), nil
}

func init() {
	RegisterStore(nameUpstream, UpstreamStoreFactory)
}

func (n *UpstreamStore) Name() string { return nameUpstream + "-" + n.name }
func (n *UpstreamStore) Has(hash string) (bool, error) {
	url := n.upstream + "/blob?hash=" + hash

	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, errors.Err(err)
	}

	res, err := n.httpClient.Do(req)
	if err != nil {
		return false, errors.Err(err)
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if res.StatusCode == http.StatusNoContent {
		return true, nil
	}
	var body []byte
	if res.Body != nil {
		body, _ = io.ReadAll(res.Body)
	}
	return false, errors.Err("upstream error. Status code: %d (%s)", res.StatusCode, string(body))
}

func (n *UpstreamStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	url := n.upstream + "/blob?hash=" + hash
	if n.edgeToken != "" {
		url += "&edge_token=" + n.edgeToken
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), n.Name()), errors.Err(err)
	}

	res, err := n.httpClient.Do(req)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), n.Name()), errors.Err(err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Errorf("Error closing response body in HTTP-GET: %s", err.Error())
		}
	}(res.Body)

	viaHeader := res.Header.Get("Via")
	var trace shared.BlobTrace
	if viaHeader != "" {
		parsedTrace, err := shared.Deserialize(viaHeader)
		if err != nil {
			return nil, shared.NewBlobTrace(time.Since(start), n.Name()), err
		}
		trace = *parsedTrace
	} else {
		trace = shared.NewBlobTrace(0, n.Name())
	}

	switch res.StatusCode {
	case http.StatusNotFound:
		return nil, trace.Stack(time.Since(start), n.Name()), ErrBlobNotFound

	case http.StatusOK:
		buffer := getBuffer()
		defer putBuffer(buffer)

		if _, err := io.Copy(buffer, res.Body); err != nil {
			return nil, trace.Stack(time.Since(start), n.Name()), errors.Err(err)
		}

		blob := make([]byte, buffer.Len())
		copy(blob, buffer.Bytes())

		metrics.MtrInBytesUpstream.Add(float64(len(blob)))
		return blob, trace.Stack(time.Since(start), n.Name()), nil

	default:
		body, _ := io.ReadAll(res.Body)
		log.Warnf("Got status code %d (%s)", res.StatusCode, string(body))
		return nil, trace.Stack(time.Since(start), n.Name()),
			errors.Err("upstream error. Status code: %d (%s)", res.StatusCode, string(body))
	}
}

func (n *UpstreamStore) Put(string, stream.Blob) error {
	return shared.ErrNotImplemented
}
func (n *UpstreamStore) PutSD(string, stream.Blob) error {
	return shared.ErrNotImplemented
}
func (n *UpstreamStore) Delete(string) error {
	return shared.ErrNotImplemented
}
func (n *UpstreamStore) Shutdown() {}

// buffer pool to reduce GC
// https://www.captaincodeman.com/2017/06/02/golang-buffer-pool-gotcha
var buffers = sync.Pool{
	// New is called when a new instance is needed
	New: func() interface{} {
		buf := make([]byte, 0, stream.MaxBlobSize)
		return bytes.NewBuffer(buf)
	},
}

// getBuffer fetches a buffer from the pool
func getBuffer() *bytes.Buffer {
	return buffers.Get().(*bytes.Buffer)
}

// putBuffer returns a buffer to the pool
func putBuffer(buf *bytes.Buffer) {
	buf.Reset()
	buffers.Put(buf)
}

func dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	return dialer.DialContext(ctx, network, address)
}

// getClient gets an http client that's customized to be more performant when dealing with blobs of 2MB in size (most of our blobs)
func getClient() *http.Client {
	// Customize the Transport to have larger connection pool
	defaultTransport := &http.Transport{
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		MaxIdleConnsPerHost:   100,
		ReadBufferSize:        stream.MaxBlobSize + 1024*10, //add an extra few KBs to make sure it fits the extra information
	}

	return &http.Client{Transport: defaultTransport}
}
