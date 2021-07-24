package store

import (
	"bytes"
	"testing"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

func TestMemStore_Put(t *testing.T) {
	s := NewMemStore()
	blob := []byte("abcdefg")
	err := s.Put("abc", blob)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
}

func TestMemStore_Get(t *testing.T) {
	s := NewMemStore()
	hash := "abc"
	blob := []byte("abcdefg")
	err := s.Put(hash, blob)
	if err != nil {
		t.Error("error getting memory blob - ", err)
	}

	gotBlob, _, err := s.Get(hash)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !bytes.Equal(gotBlob, blob) {
		t.Error("Got blob that is different from expected blob")
	}

	missingBlob, _, err := s.Get("nonexistent hash")
	if err == nil {
		t.Errorf("Expected ErrBlobNotFound, got nil")
	}
	if !errors.Is(err, ErrBlobNotFound) {
		t.Errorf("Received unexpected error: %v", err)
	}
	if !bytes.Equal(missingBlob, []byte{}) {
		t.Error("Got blob that is not empty")
	}
}
