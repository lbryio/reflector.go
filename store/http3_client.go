package store

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net/http"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// Http3Client is a client for HTTP3 blob store
type Http3Client struct {
	conn         *http.Client
	roundTripper *http3.Transport
	ServerAddr   string
}

// NewHttp3Client creates a new HTTP3 client
func NewHttp3Client(address string) (*Http3Client, error) {
	var qconf quic.Config
	window500M := 500 * 1 << 20
	qconf.MaxStreamReceiveWindow = uint64(window500M)
	qconf.MaxConnectionReceiveWindow = uint64(window500M)
	qconf.EnableDatagrams = true
	qconf.HandshakeIdleTimeout = 4 * time.Second
	qconf.MaxIdleTimeout = 20 * time.Second
	pool, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	roundTripper := &http3.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:            pool,
			InsecureSkipVerify: true,
		},
		QUICConfig: &qconf,
	}
	connection := &http.Client{
		Transport: roundTripper,
	}
	return &Http3Client{
		conn:         connection,
		roundTripper: roundTripper,
		ServerAddr:   address,
	}, nil
}

// Close closes the client
func (c *Http3Client) Close() error {
	return nil
}

// HasBlob checks if the peer has a blob
func (c *Http3Client) HasBlob(hash string) (bool, error) {
	url := c.ServerAddr + "/blob?hash=" + hash
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, errors.Err(err)
	}

	res, err := c.conn.Do(req)
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

// GetBlob gets a blob from the peer
func (c *Http3Client) GetBlob(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	url := c.ServerAddr + "/blob?hash=" + hash

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "http3"), errors.Err(err)
	}

	res, err := c.conn.Do(req)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "http3"), errors.Err(err)
	}
	defer func() { _ = res.Body.Close() }()

	viaHeader := res.Header.Get("Via")
	var trace shared.BlobTrace
	if viaHeader != "" {
		parsedTrace, err := shared.Deserialize(viaHeader)
		if err != nil {
			return nil, shared.NewBlobTrace(time.Since(start), "http3"), err
		}
		trace = *parsedTrace
	} else {
		trace = shared.NewBlobTrace(0, "http3")
	}

	switch res.StatusCode {
	case http.StatusNotFound:
		return nil, trace.Stack(time.Since(start), "http3"), ErrBlobNotFound

	case http.StatusOK:
		buffer := getBuffer()
		defer putBuffer(buffer)

		if _, err := io.Copy(buffer, res.Body); err != nil {
			return nil, trace.Stack(time.Since(start), "http3"), errors.Err(err)
		}

		blob := make([]byte, buffer.Len())
		copy(blob, buffer.Bytes())

		return blob, trace.Stack(time.Since(start), "http3"), nil

	default:
		body, _ := io.ReadAll(res.Body)
		return nil, trace.Stack(time.Since(start), "http3"),
			errors.Err("upstream error. Status code: %d (%s)", res.StatusCode, string(body))
	}
}
