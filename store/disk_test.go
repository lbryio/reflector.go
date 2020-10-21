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

func memDiskStore() *DiskBlobStore {
	d := NewDiskBlobStore("/", cacheMaxBlobs, 2)
	d.fs = afero.NewMemMapFs()
	return d
}

func countOnDisk(t *testing.T, fs afero.Fs) int {
	t.Helper()
	count := 0
	afero.Walk(fs, "/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.Fatal(err)
		}
		if !info.IsDir() {
			count++
		}
		return nil
	})
	return count
}

func TestDiskBlobStore_LRU(t *testing.T) {
	d := memDiskStore()
	b := []byte("x")
	err := d.Put("one", b)
	require.NoError(t, err)
	err = d.Put("two", b)
	require.NoError(t, err)
	err = d.Put("three", b)
	require.NoError(t, err)
	err = d.Put("four", b)
	require.NoError(t, err)
	err = d.Put("five", b)
	require.NoError(t, err)

	assert.Equal(t, cacheMaxBlobs, countOnDisk(t, d.fs))

	for k, v := range map[string]bool{
		"one":   false,
		"two":   false,
		"three": true,
		"four":  true,
		"five":  true,
		"six":   false,
	} {
		has, err := d.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	d.Get("three") // touch so it stays in cache
	d.Put("six", b)

	assert.Equal(t, cacheMaxBlobs, countOnDisk(t, d.fs))

	for k, v := range map[string]bool{
		"one":   false,
		"two":   false,
		"three": true,
		"four":  false,
		"five":  true,
		"six":   true,
	} {
		has, err := d.Has(k)
		assert.NoError(t, err)
		assert.Equal(t, v, has)
	}

	err = d.Delete("three")
	assert.NoError(t, err)
	err = d.Delete("five")
	assert.NoError(t, err)
	err = d.Delete("six")
	assert.NoError(t, err)
	assert.Equal(t, 0, countOnDisk(t, d.fs))
}

func TestDiskBlobStore_FileMissingOnDisk(t *testing.T) {
	d := memDiskStore()
	hash := "hash"
	b := []byte("this is a blob of stuff")
	err := d.Put(hash, b)
	require.NoError(t, err)

	err = d.fs.Remove("/ha/hash")
	require.NoError(t, err)

	blob, err := d.Get(hash)
	assert.Nil(t, blob)
	assert.True(t, errors.Is(err, ErrBlobNotFound), "expected (%s) %s, got (%s) %s",
		reflect.TypeOf(ErrBlobNotFound).String(), ErrBlobNotFound.Error(),
		reflect.TypeOf(err).String(), err.Error())
}
