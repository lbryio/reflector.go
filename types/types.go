package types

// SdBlob is an instance of specialized blob that contains information on the rest of the blobs it is associated with.
type SdBlob struct {
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
