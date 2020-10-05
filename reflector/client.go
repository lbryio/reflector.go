package reflector

import (
	"encoding/json"
	"log"
	"net"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"
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

// SendBlob sends a blob to the server.
func (c *Client) SendBlob(blob stream.Blob) error {
	return c.sendBlob(blob, false)
}

// SendSDBlob sends an SD blob request to the server.
func (c *Client) SendSDBlob(blob stream.Blob) error {
	return c.sendBlob(blob, true)
}

// sendBlob does the actual blob sending
func (c *Client) sendBlob(blob stream.Blob, isSDBlob bool) error {
	if !c.connected {
		return errors.Err("not connected")
	}

	if err := blob.ValidForSend(); err != nil {
		return errors.Err(err)
	}

	blobHash := blob.HashHex()
	var req sendBlobRequest
	if isSDBlob {
		req.SdBlobSize = blob.Size()
		req.SdBlobHash = blobHash
	} else {
		req.BlobSize = blob.Size()
		req.BlobHash = blobHash
	}
	sendRequest, err := json.Marshal(req)
	if err != nil {
		return err
	}

	_, err = c.conn.Write(sendRequest)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(c.conn)

	if isSDBlob {
		var sendResp sendSdBlobResponse
		err = dec.Decode(&sendResp)
		if err != nil {
			return err
		}
		if !sendResp.SendSdBlob {
			return errors.Prefix(blobHash[:8], ErrBlobExists)
		}
		log.Println("Sending SD blob " + blobHash[:8])
	} else {
		var sendResp sendBlobResponse
		err = dec.Decode(&sendResp)
		if err != nil {
			return err
		}
		if !sendResp.SendBlob {
			return errors.Prefix(blobHash[:8], ErrBlobExists)
		}
		log.Println("Sending blob " + blobHash[:8])
	}

	_, err = c.conn.Write(blob)
	if err != nil {
		return err
	}

	if isSDBlob {
		var transferResp sdBlobTransferResponse
		err = dec.Decode(&transferResp)
		if err != nil {
			return err
		}
		if !transferResp.ReceivedSdBlob {
			return errors.Err("server did not received SD blob")
		}
	} else {
		var transferResp blobTransferResponse
		err = dec.Decode(&transferResp)
		if err != nil {
			return err
		}
		if !transferResp.ReceivedBlob {
			return errors.Err("server did not received blob")
		}
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
