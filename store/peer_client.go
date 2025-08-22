package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/stream"
)

// PeerClient is a client for peer blob store
type PeerClient struct {
	conn    net.Conn
	Timeout time.Duration
}

// Connect connects to a peer
func (c *PeerClient) Connect(address string) error {
	var err error
	c.conn, err = net.DialTimeout("tcp", address, c.Timeout)
	return err
}

// Close closes the connection
func (c *PeerClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// HasBlob checks if the peer has a blob
func (c *PeerClient) HasBlob(hash string) (bool, error) {
	err := c.writeRequest("has", hash)
	if err != nil {
		return false, err
	}

	response, err := c.readResponse()
	if err != nil {
		return false, err
	}

	return response == "yes", nil
}

// GetBlob gets a blob from the peer
func (c *PeerClient) GetBlob(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	err := c.writeRequest("get", hash)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "peer"), err
	}

	response, err := c.readResponse()
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "peer"), err
	}

	if response == "no" {
		return nil, shared.NewBlobTrace(time.Since(start), "peer"), ErrBlobNotFound
	}

	size, err := binary.ReadVarint(bufio.NewReader(bytes.NewReader([]byte(response))))
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "peer"), err
	}

	blob := make([]byte, size)
	_, err = io.ReadFull(c.conn, blob)
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), "peer"), err
	}

	return blob, shared.NewBlobTrace(time.Since(start), "peer"), nil
}

func (c *PeerClient) writeRequest(cmd, hash string) error {
	_, err := c.conn.Write([]byte(cmd + " " + hash + "\n"))
	return err
}

func (c *PeerClient) readResponse() (string, error) {
	reader := bufio.NewReader(c.conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return response[:len(response)-1], nil
}
