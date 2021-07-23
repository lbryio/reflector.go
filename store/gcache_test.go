package store

import (
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cacheMaxSize = 3

func getTestGcacheStore() (*GcacheStore, *MemStore) {
	m := NewMemStore()
	return NewGcacheStore("test", m, cacheMaxSize, LFU), m
}

func TestGcacheStore_Eviction(t *testing.T) {
	lfu, mem := getTestGcacheStore()
	b := []byte("x")
	for i := 0; i < 3; i++ {
		err := lfu.Put(fmt.Sprintf("%d", i), b)
		require.NoError(t, err)
		for j := 0; j < 3-i; j++ {
			_, _, err = lfu.Get(fmt.Sprintf("%d", i))
			require.NoError(t, err)
		}
	}
	for k, v := range map[string]bool{
		"0": true,
		"1": true,
		"2": true,
	} {
		has, err := lfu.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}
	err := lfu.Put("3", b)
	require.NoError(t, err)
	for k, v := range map[string]bool{
		"0": true,
		"1": true,
		"2": false,
		"3": true,
	} {
		has, err := lfu.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}
	assert.Equal(t, cacheMaxSize, len(mem.Debug()))

	err = lfu.Delete("0")
	assert.NoError(t, err)
	err = lfu.Delete("1")
	assert.NoError(t, err)
	err = lfu.Delete("3")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(mem.Debug()))
}

func TestGcacheStore_UnderlyingBlobMissing(t *testing.T) {
	lfu, mem := getTestGcacheStore()
	hash := "hash"
	b := []byte("this is a blob of stuff")
	err := lfu.Put(hash, b)
	require.NoError(t, err)

	err = mem.Delete(hash)
	require.NoError(t, err)

	// hash still exists in lru
	assert.True(t, lfu.cache.Has(hash))

	blob, _, err := lfu.Get(hash)
	assert.Nil(t, blob)
	assert.True(t, errors.Is(err, ErrBlobNotFound), "expected (%s) %s, got (%s) %s",
		reflect.TypeOf(ErrBlobNotFound).String(), ErrBlobNotFound.Error(),
		reflect.TypeOf(err).String(), err.Error())

	// lru.Get() removes hash if underlying store doesn't have it
	assert.False(t, lfu.cache.Has(hash))
}

func TestGcacheStore_loadExisting(t *testing.T) {
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

	lfu := NewGcacheStore("test", d, 3, LFU) // lru should load existing blobs when it's created
	time.Sleep(100 * time.Millisecond)       // async load so let's wait...
	has, err := lfu.Has(hash)
	require.NoError(t, err)
	assert.True(t, has, "hash should be loaded from disk store but it's not")
}
