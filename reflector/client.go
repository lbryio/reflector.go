package reflector

import (
	"encoding/json"
	"net"

	"github.com/lbryio/lbry.go/extras/errors"
	"github.com/lbryio/lbry.go/stream"

	log "github.com/sirupsen/logrus"
)

// ErrBlobExists is a default error for when a blob already exists on the reflector server.
var ErrBlobExists = errors.Base("blob exists on server")

// Client is an instance of a client connected to a server.
type Client struct {
	conn      net.Conn
	connected bool
}

// Connect connects to a specific clients and errors if it cannot be contacted.
func (c *Client) Connect(address string) error {
	var err error
	c.conn, err = net.Dial(network, address)
	if err != nil {
		return err
	}
	c.connected = true
	return c.doHandshake(protocolVersion1)
}

// Close closes the connection with the client.
func (c *Client) Close() error {
	c.connected = false
	return c.conn.Close()
}

// SendBlob sends a send blob request to the client.
func (c *Client) SendBlob(blob stream.Blob) error {
	if !c.connected {
		return errors.Err("not connected")
	}

	if err := blob.ValidForSend(); err != nil {
		return errors.Err(err)
	}

	blobHash := blob.HashHex()
	sendRequest, err := json.Marshal(sendBlobRequest{
		BlobSize: blob.Size(),
		BlobHash: blobHash,
	})
	if err != nil {
		return err
	}

	_, err = c.conn.Write(sendRequest)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(c.conn)

	var sendResp sendBlobResponse
	err = dec.Decode(&sendResp)
	if err != nil {
		return err
	}

	if !sendResp.SendBlob {
		return errors.Prefix(blobHash[:8], ErrBlobExists)
	}

	log.Println("Sending blob " + blobHash[:8])

	_, err = c.conn.Write(blob)
	if err != nil {
		return err
	}
	var transferResp blobTransferResponse
	err = dec.Decode(&transferResp)
	if err != nil {
		return err
	}

	if !transferResp.ReceivedBlob {
		return errors.Err("server did not received blob")
	}

	return nil
}

func (c *Client) doHandshake(version int) error {
	if !c.connected {
		return errors.Err("not connected")
	}

	handshake, err := json.Marshal(handshakeRequestResponse{Version: &version})
	if err != nil {
		return err
	}

	_, err = c.conn.Write(handshake)
	if err != nil {
		return err
	}

	var resp handshakeRequestResponse
	err = json.NewDecoder(c.conn).Decode(&resp)
	if err != nil {
		return err
	} else if resp.Version == nil {
		return errors.Err("invalid handshake")
	} else if *resp.Version != version {
		return errors.Err("handshake version mismatch")
	}

	return nil
}
