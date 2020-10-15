package peer

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

// ErrBlobExists is a default error for when a blob already exists on the reflector server.
var ErrBlobExists = errors.Base("blob exists on server")

// Client is an instance of a client connected to a server.
type Client struct {
	Timeout   time.Duration
	conn      net.Conn
	buf       *bufio.Reader
	connected bool
}

// Connect connects to a specific clients and errors if it cannot be contacted.
func (c *Client) Connect(address string) error {
	var err error
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	c.conn, err = net.Dial("tcp4", address)
	if err != nil {
		return err
	}
	c.connected = true
	c.buf = bufio.NewReader(c.conn)
	return nil
}

// Close closes the connection with the client.
func (c *Client) Close() error {
	c.connected = false
	return c.conn.Close()
}

// GetStream gets a stream
func (c *Client) GetStream(sdHash string, blobCache store.BlobStore) (stream.Stream, error) {
	if !c.connected {
		return nil, errors.Err("not connected")
	}

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
	if !c.connected {
		return false, errors.Err("not connected")
	}

	sendRequest, err := json.Marshal(availabilityRequest{
		RequestedBlobs: []string{hash},
	})
	if err != nil {
		return false, err
	}

	err = c.write(sendRequest)
	if err != nil {
		return false, err
	}

	var resp availabilityResponse
	err = c.read(&resp)
	if err != nil {
		return false, err
	}

	for _, h := range resp.AvailableBlobs {
		if h == hash {
			return true, nil
		}
	}

	return false, nil
}

// GetBlob gets a blob
func (c *Client) GetBlob(hash string) (stream.Blob, error) {
	if !c.connected {
		return nil, errors.Err("not connected")
	}

	sendRequest, err := json.Marshal(blobRequest{
		RequestedBlob: hash,
	})
	if err != nil {
		return nil, err
	}

	err = c.write(sendRequest)
	if err != nil {
		return nil, err
	}

	var resp blobResponse
	err = c.read(&resp)
	if err != nil {
		return nil, err
	}

	if resp.IncomingBlob.Error != "" {
		return nil, errors.Prefix(hash[:8], resp.IncomingBlob.Error)
	}
	if resp.IncomingBlob.BlobHash != hash {
		return nil, errors.Prefix(hash[:8], "blob hash in response does not match requested hash")
	}
	if resp.IncomingBlob.Length <= 0 {
		return nil, errors.Prefix(hash[:8], "length reported as <= 0")
	}

	log.Debugf("receiving blob %s from %s", hash[:8], c.conn.RemoteAddr())

	blob, err := c.readRawBlob(resp.IncomingBlob.Length)
	if err != nil {
		return nil, err
	}
	metrics.MtrInBytesTcp.Add(float64(len(blob)))
	return blob, nil
}

func (c *Client) read(v interface{}) error {
	err := c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return errors.Err(err)
	}

	m, err := readNextMessage(c.buf)
	if err != nil {
		return err
	}

	log.Debugf("read %d bytes from %s", len(m), c.conn.RemoteAddr())

	err = json.Unmarshal(m, v)
	return errors.Err(err)
}

func (c *Client) readRawBlob(blobSize int) ([]byte, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return nil, errors.Err(err)
	}

	blob := make([]byte, blobSize)
	n, err := io.ReadFull(c.buf, blob)
	log.Debugf("read %d bytes from %s", n, c.conn.RemoteAddr())
	return blob, errors.Err(err)
}

func (c *Client) write(b []byte) error {
	err := c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return errors.Err(err)
	}

	log.Debugf("writing %d bytes to %s", len(b), c.conn.RemoteAddr())

	n, err := c.conn.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return errors.Err(err)
}
