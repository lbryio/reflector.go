package reflector

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/lbryio/lbry.go/v2/dht/bits"
	"github.com/lbryio/reflector.go/store"

	"github.com/davecgh/go-spew/spew"
	"github.com/phayes/freeport"
)

func startServerOnRandomPort(t *testing.T) (*Server, int) {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(store.NewMemStore())
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

	blob := randBlob(1000)

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

	blob := randBlob(maxBlobSize)

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

	blob := randBlob(maxBlobSize + 1)

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

	srv := NewServer(store.NewMemStore())
	srv.Timeout = testTimeout
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

	blob := randBlob(10)

	err = c.SendBlob(blob)
	t.Log(spew.Sdump(err))
	if err != io.EOF {
		t.Error("server should have timed out by now")
	}
}

//func TestServer_InvalidJSONHandshake(t *testing.T) {
//	srv, port := startServerOnRandomPort(t)
//	defer srv.Shutdown()
//
//	c := Client{}
//	err := c.Connect(":" + strconv.Itoa(port))
//	if err != nil {
//		t.Fatal("error connecting client to server", err)
//	}
//
//	_, err = c.conn.Write([]byte(`{"stuff":4,tf}"`))
//	if err == nil {
//		t.Error("expected an error")
//	}
//}

type mockPartialStore struct {
	*store.MemStore
	missing []string
}

func (m mockPartialStore) MissingBlobsForKnownStream(hash string) ([]string, error) {
	return m.missing, nil
}

func TestServer_PartialUpload(t *testing.T) {
	port, err := freeport.GetFreePort()
	if err != nil {
		t.Fatal(err)
	}

	sdHash := bits.Rand().String()
	missing := make([]string, 4)
	for i := range missing {
		missing[i] = bits.Rand().String()
	}

	st := store.BlobStore(&mockPartialStore{MemStore: store.NewMemStore(), missing: missing})
	if _, ok := st.(neededBlobChecker); !ok {
		t.Fatal("mock does not implement the relevant interface")
	}
	err = st.Put(sdHash, randBlob(10))
	if err != nil {
		t.Fatal(err)
	}

	srv := NewServer(st)
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

	sendRequest, err := json.Marshal(sendBlobRequest{
		SdBlobHash: sdHash,
		SdBlobSize: len(sdHash),
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.conn.Write(sendRequest)
	if err != nil {
		t.Fatal(err)
	}

	var sendResp sendSdBlobResponse
	err = json.NewDecoder(c.conn).Decode(&sendResp)
	if err != nil {
		t.Fatal(err)
	}

	if sendResp.SendSdBlob {
		t.Errorf("expected SendSdBlob = false, got true")
	}

	if len(sendResp.NeededBlobs) != len(missing) {
		t.Fatalf("got %d needed blobs, expected %d", len(sendResp.NeededBlobs), len(missing))
	}

	sort.Strings(sendResp.NeededBlobs)
	sort.Strings(missing)

	for i := range missing {
		if missing[i] != sendResp.NeededBlobs[i] {
			t.Errorf("needed blobs mismatch: %s != %s", missing[i], sendResp.NeededBlobs[i])
		}
	}
}

func randBlob(size int) []byte {
	//if size > maxBlobSize {
	//	panic("blob size too big")
	//}
	blob := make([]byte, size)
	_, err := rand.Read(blob)
	if err != nil {
		panic("failed to make random blob")
	}
	return blob
}
