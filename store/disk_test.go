package store

import (
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiskStore_Get(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "reflector_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	d := NewDiskStore(tmpDir, 2)

	hash := "1234567890"
	data := []byte("oyuntyausntoyaunpdoyruoyduanrstjwfjyuwf")

	expectedPath := path.Join(tmpDir, hash[:2], hash)
	err = os.MkdirAll(filepath.Dir(expectedPath), os.ModePerm)
	require.NoError(t, err)
	err = ioutil.WriteFile(expectedPath, data, os.ModePerm)
	require.NoError(t, err)

	blob, _, err := d.Get(hash)
	assert.NoError(t, err)
	assert.EqualValues(t, data, blob)
}

func TestDiskStore_GetNonexistentBlob(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "reflector_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)
	d := NewDiskStore(tmpDir, 2)

	blob, _, err := d.Get("nonexistent")
	assert.Nil(t, blob)
	assert.True(t, errors.Is(err, ErrBlobNotFound))
}
