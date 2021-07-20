package http3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/lucas-clemente/quic-go/http3"
	log "github.com/sirupsen/logrus"
)

// Client is an instance of a client connected to a server.
type Client struct {
	Timeout      time.Duration
	conn         *http.Client
	roundTripper *http3.RoundTripper
	ServerAddr   string
}

// Close closes the connection with the client.
func (c *Client) Close() error {
	c.conn.CloseIdleConnections()
	return c.roundTripper.Close()
}

// GetStream gets a stream
func (c *Client) GetStream(sdHash string, blobCache store.BlobStore) (stream.Stream, error) {
	var sd stream.SDBlob

	b, _, err := c.GetBlob(sdHash)
	if err != nil {
		return nil, err
	}

	err = sd.FromBlob(b)
	if err != nil {
		return nil, err
	}

	s := make(stream.Stream, len(sd.BlobInfos)+1-1) // +1 for sd blob, -1 for last null blob
	s[0] = b

	for i := 0; i < len(sd.BlobInfos)-1; i++ {
		var trace shared.BlobTrace
		s[i+1], trace, err = c.GetBlob(hex.EncodeToString(sd.BlobInfos[i].BlobHash))
		if err != nil {
			return nil, err
		}
		log.Debug(trace.String())
	}

	return s, nil
}

// HasBlob checks if the blob is available
func (c *Client) HasBlob(hash string) (bool, error) {
	resp, err := c.conn.Get(fmt.Sprintf("https://%s/has/%s", c.ServerAddr, hash))
	if err != nil {
		return false, errors.Err(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, errors.Err("non 200 status code returned: %d", resp.StatusCode)
}

// GetBlob gets a blob
func (c *Client) GetBlob(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	resp, err := c.conn.Get(fmt.Sprintf("https://%s/get/%s?trace=true", c.ServerAddr, hash))
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "http3"), errors.Err(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Printf("%s blob not found %d\n", hash, resp.StatusCode)
		return nil, shared.NewBlobTrace(time.Since(start), "http3"), errors.Err(store.ErrBlobNotFound)
	} else if resp.StatusCode != http.StatusOK {
		return nil, shared.NewBlobTrace(time.Since(start), "http3"), errors.Err("non 200 status code returned: %d", resp.StatusCode)
	}

	tmp := getBuffer()
	defer putBuffer(tmp)
	serialized := resp.Header.Get("Via")
	trace := shared.NewBlobTrace(time.Since(start), "http3")
	if serialized != "" {
		parsedTrace, err := shared.Deserialize(serialized)
		if err != nil {
			return nil, shared.NewBlobTrace(time.Since(start), "http3"), err
		}
		trace = *parsedTrace
	}
	written, err := io.Copy(tmp, resp.Body)
	if err != nil {
		return nil, trace.Stack(time.Since(start), "http3"), errors.Err(err)
	}

	blob := make([]byte, written)
	copy(blob, tmp.Bytes())

	metrics.MtrInBytesUdp.Add(float64(len(blob)))

	return blob, trace.Stack(time.Since(start), "http3"), nil
}

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
