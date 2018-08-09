package reflector

import (
	"crypto/rand"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/lbryio/reflector.go/store"

	"github.com/phayes/freeport"
)

func startServerOnRandomPort(t *testing.T) (*Server, int) {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(&store.MemoryBlobStore{})
	err = srv.Start("127.0.0.1:" + strconv.Itoa(port))
	if err != nil {
		t.Fatal(err)
	}

	return srv, port
}

func TestClient_NotConnected(t *testing.T) {
	c := Client{}
	err := c.SendBlob([]byte{})
	if err == nil {
		t.Error("client should error if it is not connected")
	}
}

func TestClient_EmptyBlob(t *testing.T) {
	srv, port := startServerOnRandomPort(t)
	defer srv.Shutdown()

	c := Client{}
	err := c.Connect(":" + strconv.Itoa(port))
	if err != nil {
		t.Fatal("error connecting client to server", err)
	}

	err = c.SendBlob([]byte{})
	if err == nil {
		t.Error("client should not send empty blob")
	}
}

func TestServer_MediumBlob(t *testing.T) {
	srv, port := startServerOnRandomPort(t)
	defer srv.Shutdown()

	c := Client{}
	err := c.Connect(":" + strconv.Itoa(port))
	if err != nil {
		t.Fatal("error connecting client to server", err)
	}

	blob := make([]byte, 1000)
	_, err = rand.Read(blob)
	if err != nil {
		t.Fatal("failed to make random blob")
	}

	err = c.SendBlob(blob)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_FullBlob(t *testing.T) {
	srv, port := startServerOnRandomPort(t)
	defer srv.Shutdown()

	c := Client{}
	err := c.Connect(":" + strconv.Itoa(port))
	if err != nil {
		t.Fatal("error connecting client to server", err)
	}

	blob := make([]byte, maxBlobSize)
	_, err = rand.Read(blob)
	if err != nil {
		t.Fatal("failed to make random blob")
	}

	err = c.SendBlob(blob)
	if err != nil {
		t.Error(err)
	}
}

func TestServer_TooBigBlob(t *testing.T) {
	srv, port := startServerOnRandomPort(t)
	defer srv.Shutdown()

	c := Client{}
	err := c.Connect(":" + strconv.Itoa(port))
	if err != nil {
		t.Fatal("error connecting client to server", err)
	}

	blob := make([]byte, maxBlobSize+1)
	_, err = rand.Read(blob)
	if err != nil {
		t.Fatal("failed to make random blob")
	}

	err = c.SendBlob(blob)
	if err == nil {
		t.Error("server should reject blob above max size")
	}
}

func TestServer_Timeout(t *testing.T) {
	t.Skip("server and client have no way to detect errors right now")

	testTimeout := 50 * time.Millisecond

	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(&store.MemoryBlobStore{})
	srv.timeout = testTimeout
	err = srv.Start("127.0.0.1:" + strconv.Itoa(port))
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Shutdown()

	c := Client{}
	err = c.Connect(":" + strconv.Itoa(port))
	if err != nil {
		t.Fatal("error connecting client to server", err)
	}

	time.Sleep(testTimeout * 2)

	blob := make([]byte, 10)
	_, err = rand.Read(blob)
	if err != nil {
		t.Fatal("failed to make random blob")
	}

	err = c.SendBlob(blob)
	t.Log(spew.Sdump(err))
	if err != io.EOF {
		t.Error("server should have timed out by now")
	}
}
