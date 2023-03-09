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
)

// HttpStore is a store that works on top of the HTTP protocol
type HttpStore struct {
	upstream   string
	httpClient *http.Client
	edgeToken  string
}

func NewHttpStore(upstream, edgeToken string) *HttpStore {
	return &HttpStore{
		upstream:   "http://" + upstream,
		httpClient: getClient(),
		edgeToken:  edgeToken,
	}
}

const nameHttp = "http"

func (n *HttpStore) Name() string { return nameHttp }
func (n *HttpStore) Has(hash string) (bool, error) {
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

func (n *HttpStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	url := n.upstream + "/blob?hash=" + hash
	if n.edgeToken != "" {
		url += "&edge_token=" + n.edgeToken
	}

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), n.Name()), errors.Err(err)
	}

	res, err := n.httpClient.Do(req)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), n.Name()), errors.Err(err)
	}
	defer func() { _ = res.Body.Close() }()
	tmp := getBuffer()
	defer putBuffer(tmp)
	serialized := res.Header.Get("Via")
	trace := shared.NewBlobTrace(time.Since(start), n.Name())
	if serialized != "" {
		parsedTrace, err := shared.Deserialize(serialized)
		if err != nil {
			return nil, shared.NewBlobTrace(time.Since(start), n.Name()), err
		}
		trace = *parsedTrace
	}

	if res.StatusCode == http.StatusNotFound {
		return nil, trace.Stack(time.Since(start), n.Name()), ErrBlobNotFound
	}
	if res.StatusCode == http.StatusOK {
		written, err := io.Copy(tmp, res.Body)
		if err != nil {
			return nil, trace.Stack(time.Since(start), n.Name()), errors.Err(err)
		}

		blob := make([]byte, written)
		copy(blob, tmp.Bytes())
		metrics.MtrInBytesHttp.Add(float64(len(blob)))
		return blob, trace.Stack(time.Since(start), n.Name()), nil
	}
	var body []byte
	if res.Body != nil {
		body, _ = io.ReadAll(res.Body)
	}

	return nil, trace.Stack(time.Since(start), n.Name()), errors.Err("upstream error. Status code: %d (%s)", res.StatusCode, string(body))
}

func (n *HttpStore) Put(string, stream.Blob) error {
	return shared.ErrNotImplemented
}
func (n *HttpStore) PutSD(string, stream.Blob) error {
	return shared.ErrNotImplemented
}
func (n *HttpStore) Delete(string) error {
	return shared.ErrNotImplemented
}
func (n *HttpStore) Shutdown() {}

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
