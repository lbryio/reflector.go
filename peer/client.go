package peer

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"time"

	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/stream"

	"github.com/lbryio/lbry.go/extras/errors"

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

// WriteStream downloads and writes a stream to file
func (c *Client) WriteStream(sdHash, dir string, blobStore store.BlobStore) error {
	if !c.connected {
		return errors.Err("not connected")
	}

	var sd stream.SDBlob

	sdb, err := c.getBlobWithCache(sdHash, blobStore)
	if err != nil {
		return err
	}

	err = sd.FromBlob(sdb)
	if err != nil {
		return err
	}

	info, err := os.Stat(dir)
	if err != nil {
		return errors.Prefix("cannot stat "+dir, err)
	} else if !info.IsDir() {
		return errors.Err(dir + " must be a directory")
	}

	f, err := os.Create(dir + "/" + sd.SuggestedFileName)
	if err != nil {
		return err
	}

	for i := 0; i < len(sd.BlobInfos)-1; i++ {
		b, err := c.getBlobWithCache(hex.EncodeToString(sd.BlobInfos[i].BlobHash), blobStore)
		if err != nil {
			return err
		}

		data, err := b.Plaintext(sd.Key, sd.BlobInfos[i].IV)
		if err != nil {
			return err
		}

		_, err = f.Write(data)
		if err != nil {
			return err
		}
	}

	return nil
}

// GetStream gets a stream
func (c *Client) GetStream(sdHash string, blobCache store.BlobStore) (stream.Stream, error) {
	if !c.connected {
		return nil, errors.Err("not connected")
	}

	var sd stream.SDBlob

	b, err := c.getBlobWithCache(sdHash, blobCache)
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
		s[i+1], err = c.getBlobWithCache(hex.EncodeToString(sd.BlobInfos[i].BlobHash), blobCache)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (c *Client) getBlobWithCache(hash string, blobCache store.BlobStore) (stream.Blob, error) {
	if blobCache == nil {
		return c.GetBlob(hash)
	}

	blob, err := blobCache.Get(hash)
	if err == nil || !errors.Is(err, store.ErrBlobNotFound) {
		return blob, err
	}

	blob, err = c.GetBlob(hash)
	if err != nil {
		return nil, err
	}

	err = blobCache.Put(hash, blob)

	return blob, err
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
		return nil, errors.Prefix(hash[:8], "Blob hash in response does not match requested hash")
	}
	if resp.IncomingBlob.Length <= 0 {
		return nil, errors.Prefix(hash[:8], "Length reported as <= 0")
	}

	log.Println("Receiving blob " + hash[:8])

	blob, err := c.readRawBlob(resp.IncomingBlob.Length)
	if err != nil {
		return nil, err
	}

	return stream.Blob(blob), nil
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

	err = json.Unmarshal(m, v)
	return errors.Err(err)
}

func (c *Client) readRawBlob(blobSize int) ([]byte, error) {
	err := c.conn.SetReadDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return nil, errors.Err(err)
	}

	blob := make([]byte, blobSize)
	_, err = io.ReadFull(c.buf, blob)
	return blob, errors.Err(err)
}

func (c *Client) write(b []byte) error {
	err := c.conn.SetWriteDeadline(time.Now().Add(c.Timeout))
	if err != nil {
		return errors.Err(err)
	}

	log.Debugf("Writing %d bytes", len(b))

	n, err := c.conn.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return errors.Err(err)
}
