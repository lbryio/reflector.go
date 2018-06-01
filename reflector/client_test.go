package reflector

import (
	"crypto/rand"
	"io/ioutil"
	"os"
	"strconv"
	"testing"

	"github.com/lbryio/reflector.go/store"
	log "github.com/sirupsen/logrus"
)

var address = "localhost:" + strconv.Itoa(DefaultPort)

func removeAll(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Panic("error removing files and directory - ", err)
	}
}

func TestMain(m *testing.M) {
	dir, err := ioutil.TempDir("", "reflector_client_test")
	if err != nil {
		log.Panic("could not create temp directory - ", err)
	}
	defer removeAll(dir)

	ms := store.MemoryBlobStore{}
	s := NewServer(&ms)
	go func() {
		if err := s.ListenAndServe(address); err != nil {
			log.Panic("error starting up reflector server - ", err)
		}
	}()

	os.Exit(m.Run())
}

func TestNotConnected(t *testing.T) {
	c := Client{}
	err := c.SendBlob([]byte{})
	if err == nil {
		t.Error("client should error if it is not connected")
	}
}

func TestSmallBlob(t *testing.T) {
	c := Client{}
	err := c.Connect(address)
	if err != nil {
		t.Error("error connecting client to server - ", err)
	}

	err = c.SendBlob([]byte{})
	if err == nil {
		t.Error("client should error if blob is empty")
	}

	blob := make([]byte, 1000)
	_, err = rand.Read(blob)
	if err != nil {
		t.Error("failed to make random blob")
	}

	err = c.SendBlob([]byte{})
	if err == nil {
		t.Error("client should error if blob is the wrong size")
	}
}
