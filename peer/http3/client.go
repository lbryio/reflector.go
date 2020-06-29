package http3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/lucas-clemente/quic-go/http3"
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
	return c.roundTripper.Close()
}

// GetStream gets a stream
func (c *Client) GetStream(sdHash string, blobCache store.BlobStore) (stream.Stream, error) {
	var sd stream.SDBlob

	b, err := c.GetBlob(sdHash)
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
		s[i+1], err = c.GetBlob(hex.EncodeToString(sd.BlobInfos[i].BlobHash))
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// HasBlob checks if the blob is available
func (c *Client) HasBlob(hash string) (bool, error) {
	resp, err := c.conn.Get(fmt.Sprintf("https://%s/has/%s", c.ServerAddr, hash))
	if err != nil {
		return false, errors.Err(err)
	}
	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, errors.Err("non 200 status code returned: %d", resp.StatusCode)
}

// GetBlob gets a blob
func (c *Client) GetBlob(hash string) (stream.Blob, error) {
	resp, err := c.conn.Get(fmt.Sprintf("https://%s/get/%s", c.ServerAddr, hash))
	if err != nil {
		return nil, errors.Err(err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.Err(store.ErrBlobNotFound)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Err("non 200 status code returned: %d", resp.StatusCode)
	}
	body := &bytes.Buffer{}
	_, err = io.Copy(body, resp.Body)
	if err != nil {
		return nil, errors.Err(err)
	}
	return body.Bytes(), nil
}
