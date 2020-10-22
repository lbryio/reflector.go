package store

import (
	"os"
	"reflect"
	"testing"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cacheMaxBlobs = 3

func testLRUStore() (*LRUStore, *DiskBlobStore) {
	d := NewDiskBlobStore("/", 2)
	d.fs = afero.NewMemMapFs()
	return NewLRUStore(d, 3), d
}

func countOnDisk(t *testing.T, disk *DiskBlobStore) int {
	t.Helper()

	count := 0
	afero.Walk(disk.fs, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})

	list, err := disk.list()
	require.NoError(t, err)
	require.Equal(t, count, len(list))

	return count
}

func TestLRUStore_Eviction(t *testing.T) {
	lru, disk := testLRUStore()
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

	assert.Equal(t, cacheMaxBlobs, countOnDisk(t, disk))

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

	assert.Equal(t, cacheMaxBlobs, countOnDisk(t, disk))

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
	assert.Equal(t, 0, countOnDisk(t, disk))
}

func TestLRUStore_UnderlyingBlobMissing(t *testing.T) {
	lru, disk := testLRUStore()
	hash := "hash"
	b := []byte("this is a blob of stuff")
	err := lru.Put(hash, b)
	require.NoError(t, err)

	err = disk.fs.Remove("/ha/hash")
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
