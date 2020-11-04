package store

import (
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cacheMaxBlobs = 3

func getTestLRUStore() (*LRUStore, *MemStore) {
	m := NewMemStore()
	return NewLRUStore("test", m, 3), m
}

func TestLRUStore_Eviction(t *testing.T) {
	lru, mem := getTestLRUStore()
	b := []byte("x")
	err := lru.Put("one", b)
	require.NoError(t, err)
	err = lru.Put("two", b)
	require.NoError(t, err)
	err = lru.Put("three", b)
	require.NoError(t, err)
	err = lru.Put("four", b)
	require.NoError(t, err)
	err = lru.Put("five", b)
	require.NoError(t, err)

	assert.Equal(t, cacheMaxBlobs, len(mem.Debug()))

	for k, v := range map[string]bool{
		"one":   false,
		"two":   false,
		"three": true,
		"four":  true,
		"five":  true,
		"six":   false,
	} {
		has, err := lru.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	lru.Get("three") // touch so it stays in cache
	lru.Put("six", b)

	assert.Equal(t, cacheMaxBlobs, len(mem.Debug()))

	for k, v := range map[string]bool{
		"one":   false,
		"two":   false,
		"three": true,
		"four":  false,
		"five":  true,
		"six":   true,
	} {
		has, err := lru.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	err = lru.Delete("three")
	assert.NoError(t, err)
	err = lru.Delete("five")
	assert.NoError(t, err)
	err = lru.Delete("six")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mem.Debug()))
}

func TestLRUStore_UnderlyingBlobMissing(t *testing.T) {
	lru, mem := getTestLRUStore()
	hash := "hash"
	b := []byte("this is a blob of stuff")
	err := lru.Put(hash, b)
	require.NoError(t, err)

	err = mem.Delete(hash)
	require.NoError(t, err)

	// hash still exists in lru
	assert.True(t, lru.lru.Contains(hash))

	blob, err := lru.Get(hash)
	assert.Nil(t, blob)
	assert.True(t, errors.Is(err, ErrBlobNotFound), "expected (%s) %s, got (%s) %s",
		reflect.TypeOf(ErrBlobNotFound).String(), ErrBlobNotFound.Error(),
		reflect.TypeOf(err).String(), err.Error())

	// lru.Get() removes hash if underlying store doesn't have it
	assert.False(t, lru.lru.Contains(hash))
}

func TestLRUStore_loadExisting(t *testing.T) {
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

	lru := NewLRUStore("test", d, 3) // lru should load existing blobs when it's created
	has, err := lru.Has(hash)
	require.NoError(t, err)
	assert.True(t, has, "hash should be loaded from disk store but it's not")
}
