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
	protocolVersion2 = 2
)

var ErrBlobExists = fmt.Errorf("Blob exists on server")

type errorResponse struct {
	Error string `json:"error"`
}

type handshakeRequestResponse struct {
	Version int `json:"version"`
}

type sendBlobRequest struct {
	BlobHash   string `json:"blob_hash,omitempty"`
	BlobSize   int    `json:"blob_size,omitempty"`
	SdBlobHash string `json:"sd_blob_hash,omitempty"`
	SdBlobSize int    `json:"sd_blob_size,omitempty"`
}

type sendBlobResponse struct {
	SendBlob bool `json:"send_blob"`
}

type sendSdBlobResponse struct {
	SendSdBlob  bool     `json:"send_sd_blob"`
	NeededBlobs []string `json:"needed_blobs,omitempty"`
}

type blobTransferResponse struct {
	ReceivedBlob bool `json:"received_blob"`
}

type sdBlobTransferResponse struct {
	ReceivedSdBlob bool `json:"received_sd_blob"`
}

func getBlobHash(blob []byte) string {
	hashBytes := sha512.Sum384(blob)
	return hex.EncodeToString(hashBytes[:])
}
