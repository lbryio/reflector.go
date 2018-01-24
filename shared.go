package main

import (
	"crypto/sha512"
	"encoding/hex"

	"github.com/lbryio/errors.go"
)

const (
	DefaultPort = 5566

	BlobSize = 2 * 1024 * 1024

	protocolVersion1 = 0
	protocolVersion2 = 1
)

var ErrBlobExists = errors.Base("blob exists on server")

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

// can be used to read the sd blob and then return a list of blobs that are actually missing
type sdBlobContents struct {
	StreamName string `json:"stream_name"`
	Blobs      []struct {
		Length   int    `json:"length"`
		BlobNum  int    `json:"blob_num"`
		BlobHash string `json:"blob_hash,omitempty"`
		Iv       string `json:"iv"`
	} `json:"blobs"`
	StreamType        string `json:"stream_type"`
	Key               string `json:"key"`
	SuggestedFileName string `json:"suggested_file_name"`
	StreamHash        string `json:"stream_hash"`
}
