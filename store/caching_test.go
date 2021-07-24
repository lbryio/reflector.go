package store

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/stream"
)

func TestCachingStore_Put(t *testing.T) {
	origin := NewMemStore()
	cache := NewMemStore()
	s := NewCachingStore("test", origin, cache)

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

func TestCachingStore_CacheMiss(t *testing.T) {
	origin := NewMemStore()
	cache := NewMemStore()
	s := NewCachingStore("test", origin, cache)

	b := []byte("this is a blob of stuff")
	hash := "hash"
	err := origin.Put(hash, b)
	if err != nil {
		t.Fatal(err)
	}

	res, stack, err := s.Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, res) {
		t.Errorf("expected Get() to return %s, got %s", string(b), string(res))
	}
	time.Sleep(10 * time.Millisecond) //storing to cache is done async so let's give it some time

	has, err := cache.Has(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Errorf("Get() did not copy blob to cache")
	}
	t.Logf("stack: %s", stack.String())

	res, stack, err = cache.Get(hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, res) {
		t.Errorf("expected cached Get() to return %s, got %s", string(b), string(res))
	}
	t.Logf("stack: %s", stack.String())
}

func TestCachingStore_ThunderingHerd(t *testing.T) {
	storeDelay := 100 * time.Millisecond
	origin := NewSlowBlobStore(storeDelay)
	cache := NewMemStore()
	s := NewCachingStore("test", origin, cache)

	b := []byte("this is a blob of stuff")
	hash := "hash"
	err := origin.Put(hash, b)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}

	getNoErr := func() {
		res, _, err := s.Get(hash)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, res) {
			t.Errorf("expected Get() to return %s, got %s", string(b), string(res))
		}
		wg.Done()
	}

	start := time.Now()

	wg.Add(4)
	go func() {
		go getNoErr()
		time.Sleep(10 * time.Millisecond)
		go getNoErr()
		time.Sleep(10 * time.Millisecond)
		go getNoErr()
		time.Sleep(10 * time.Millisecond)
		go getNoErr()
	}()

	wg.Wait()
	duration := time.Since(start)

	// only the first getNoErr() should hit the origin. the rest should wait for the first request to return
	// once the first returns, the others should return immediately
	// therefore, if the delay much longer than 100ms, it means subsequent requests also went to the origin
	expectedMaxDelay := storeDelay + 5*time.Millisecond // a bit of extra time to let requests finish
	if duration > expectedMaxDelay {
		t.Errorf("Expected delay of at most %s, got %s", expectedMaxDelay, duration)
	}
}

// SlowBlobStore adds a delay to each request
type SlowBlobStore struct {
	mem   *MemStore
	delay time.Duration
}

func NewSlowBlobStore(delay time.Duration) *SlowBlobStore {
	return &SlowBlobStore{
		mem:   NewMemStore(),
		delay: delay,
	}
}
func (s *SlowBlobStore) Name() string {
	return "slow"
}

func (s *SlowBlobStore) Has(hash string) (bool, error) {
	time.Sleep(s.delay)
	return s.mem.Has(hash)
}

func (s *SlowBlobStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	time.Sleep(s.delay)
	return s.mem.Get(hash)
}

func (s *SlowBlobStore) Put(hash string, blob stream.Blob) error {
	time.Sleep(s.delay)
	return s.mem.Put(hash, blob)
}

func (s *SlowBlobStore) PutSD(hash string, blob stream.Blob) error {
	time.Sleep(s.delay)
	return s.mem.PutSD(hash, blob)
}

func (s *SlowBlobStore) Delete(hash string) error {
	time.Sleep(s.delay)
	return s.mem.Delete(hash)
}

func (s *SlowBlobStore) Shutdown() {
	return
}
