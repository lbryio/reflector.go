package store

type BlobStore interface {
	Has(string) (bool, error)
	Get(string) ([]byte, error)
	Put(string, []byte) error
	PutSD(string, []byte) error
}
