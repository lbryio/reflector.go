package main

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
)

const (
	DefaultPort = 5566

	BlobSize = 2 * 1024 * 1024

	protocolVersion1 = 1
	protocolVersion2 = 2 // not implemented
)

var ErrBlobExists = fmt.Errorf("Blob exists on server")

type errorResponse struct {
	Error string `json:"error"`
}

type handshakeRequestResponse struct {
	Version int `json:"version"`
}

type sendBlobRequest struct {
	BlobHash string `json:"blob_hash"`
	BlobSize int    `json:"blob_size"`
}

type sendBlobResponse struct {
	SendBlob bool `json:"send_blob"`
}

type blobTransferResponse struct {
	ReceivedBlob bool `json:"received_blob"`
}

func getBlobHash(blob []byte) string {
	hashBytes := sha512.Sum384(blob)
	return hex.EncodeToString(hashBytes[:])
}
