package store

import (
	"bytes"
	"testing"
)

func TestCachingBlobStore_Put(t *testing.T) {
	origin := NewMemoryBlobStore()
	cache := NewMemoryBlobStore()
	s := NewCachingBlobStore(origin, cache)

	b := []byte("this is a blob of stuff")
	hash := "hash"

	err := s.Put(hash, b)
	if err != nil {
		t.Fatal(err)
	}

	has, err := origin.Has(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("failed to store blob in origin")
	}

	has, err = cache.Has(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("failed to store blob in cache")
	}
}

func TestCachingBlobStore_CacheMiss(t *testing.T) {
	origin := NewMemoryBlobStore()
	cache := NewMemoryBlobStore()
	s := NewCachingBlobStore(origin, cache)

	b := []byte("this is a blob of stuff")
	hash := "hash"
	err := origin.Put(hash, b)
	if err != nil {
		t.Fatal(err)
	}

	res, err := s.Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, res) {
		t.Errorf("expected Get() to return %s, got %s", string(b), string(res))
	}

	has, err := cache.Has(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("Get() did not copy blob to cache")
	}

	res, err = cache.Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, res) {
		t.Errorf("expected cached Get() to return %s, got %s", string(b), string(res))
	}
}
