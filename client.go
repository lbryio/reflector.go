package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
)

type Client struct {
	conn net.Conn
}

func (c *Client) Connect(address string) error {
	var err error
	c.conn, err = net.Dial("tcp", address)
	if err != nil {
		return err
	}
	return c.doHandshake(protocolVersion1)
}
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) SendBlob(blob []byte) error {
	if len(blob) != BlobSize {
		return fmt.Errorf("Blob must be exactly " + strconv.Itoa(BlobSize) + " bytes")
	}

	blobHash := getBlobHash(blob)
	sendRequest, err := json.Marshal(sendBlobRequest{
		BlobSize: len(blob),
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
		return ErrBlobExists
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
		return fmt.Errorf("Server did not received blob")
	}

	return nil
}

func (c *Client) doHandshake(version int) error {
	handshake, err := json.Marshal(handshakeRequestResponse{Version: version})
	if err != nil {
		return err
	}

	_, err = c.conn.Write(handshake)
	if err != nil {
		return err
	}

	var resp handshakeRequestResponse
	dec := json.NewDecoder(c.conn)
	err = dec.Decode(&resp)
	if err != nil {
		return err
	} else if resp.Version != version {
		return fmt.Errorf("Handshake version mismatch")
	}

	return nil
}
