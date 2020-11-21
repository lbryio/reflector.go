package store

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cacheMaxSize = 3

func getTestLFUDAStore() (*LFUDAStore, *MemStore) {
	m := NewMemStore()
	return NewLFUDAStore("test", m, cacheMaxSize), m
}

func TestFUDAStore_Eviction(t *testing.T) {
	lfuda, mem := getTestLFUDAStore()
	b := []byte("x")
	err := lfuda.Put("one", b)
	require.NoError(t, err)
	err = lfuda.Put("two", b)
	require.NoError(t, err)
	err = lfuda.Put("three", b)
	require.NoError(t, err)
	err = lfuda.Put("four", b)
	require.NoError(t, err)
	err = lfuda.Put("five", b)
	require.NoError(t, err)
	err = lfuda.Put("five", b)
	require.NoError(t, err)
	err = lfuda.Put("four", b)
	require.NoError(t, err)
	err = lfuda.Put("two", b)
	require.NoError(t, err)

	_, err = lfuda.Get("five")
	require.NoError(t, err)
	_, err = lfuda.Get("four")
	require.NoError(t, err)
	_, err = lfuda.Get("two")
	require.NoError(t, err)
	assert.Equal(t, cacheMaxBlobs, len(mem.Debug()))

	for k, v := range map[string]bool{
		"one":   false,
		"two":   true,
		"three": false,
		"four":  true,
		"five":  true,
		"six":   false,
	} {
		has, err := lfuda.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	lfuda.Get("two")  // touch so it stays in cache
	lfuda.Get("five") // touch so it stays in cache
	lfuda.Put("six", b)

	assert.Equal(t, cacheMaxBlobs, len(mem.Debug()))

	keys := lfuda.lfuda.Keys()
	log.Infof("%+v", keys)
	for k, v := range map[string]bool{
		"one":   false,
		"two":   true,
		"three": false,
		"four":  false,
		"five":  true,
		"six":   true,
	} {
		has, err := lfuda.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	err = lfuda.Delete("six")
	assert.NoError(t, err)
	err = lfuda.Delete("five")
	assert.NoError(t, err)
	err = lfuda.Delete("two")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mem.Debug()))
}

func TestFUDAStore_UnderlyingBlobMissing(t *testing.T) {
	lfuda, mem := getTestLFUDAStore()
	hash := "hash"
	b := []byte("this is a blob of stuff")
	err := lfuda.Put(hash, b)
	require.NoError(t, err)

	err = mem.Delete(hash)
	require.NoError(t, err)

	// hash still exists in lru
	assert.True(t, lfuda.lfuda.Contains(hash))

	blob, err := lfuda.Get(hash)
	assert.Nil(t, blob)
	assert.True(t, errors.Is(err, ErrBlobNotFound), "expected (%s) %s, got (%s) %s",
		reflect.TypeOf(ErrBlobNotFound).String(), ErrBlobNotFound.Error(),
		reflect.TypeOf(err).String(), err.Error())

	// lru.Get() removes hash if underlying store doesn't have it
	assert.False(t, lfuda.lfuda.Contains(hash))
}

func TestFUDAStore_loadExisting(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "reflector_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	d := NewDiskStore(tmpDir, 2)

	hash := "hash"
	b := []byte("this is a blob of stuff")
	err = d.Put(hash, b)
	require.NoError(t, err)

	existing, err := d.list()
	require.NoError(t, err)
	require.Equal(t, 1, len(existing), "blob should exist in cache")
	assert.Equal(t, hash, existing[0])

	lfuda := NewLFUDAStore("test", d, 3) // lru should load existing blobs when it's created
	time.Sleep(100 * time.Millisecond)   // async load so let's wait...
	has, err := lfuda.Has(hash)
	require.NoError(t, err)
	assert.True(t, has, "hash should be loaded from disk store but it's not")
}
