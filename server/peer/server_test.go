package peer

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/lbryio/reflector.go/store"
)

var blobs = map[string][]byte{
	"a": []byte("abcdefg"),
	"b": []byte("hijklmn"),
	"c": []byte("opqrstu"),
}

type pair struct {
	request  []byte
	response []byte
}

var availabilityRequests = []pair{
	{
		request:  []byte(`{"lbrycrd_address":true,"requested_blobs":["a","b"]}`),
		response: []byte(`{"lbrycrd_address":"` + LbrycrdAddress + `","available_blobs":["a","b"]}`),
	},
	{
		request:  []byte(`{"lbrycrd_address":true,"requested_blobs":["x","a","y"]}`),
		response: []byte(`{"lbrycrd_address":"` + LbrycrdAddress + `","available_blobs":["a"]}`),
	},
	{
		request:  []byte(`{"lbrycrd_address":true,"requested_blobs":[]}`),
		response: []byte(`{"lbrycrd_address":"` + LbrycrdAddress + `","available_blobs":[]}`),
	},
}

func getServer(t *testing.T, withBlobs bool) *Server {
	st := store.NewMemStore()
	if withBlobs {
		for k, v := range blobs {
			err := st.Put(k, v)
			if err != nil {
				t.Error("error during put operation of memory blobstore - ", err)
			}
		}
	}
	return NewServer(st)
}

func TestAvailabilityRequest_NoBlobs(t *testing.T) {
	s := getServer(t, false)

	for _, p := range availabilityRequests {
		response, err := s.handleAvailabilityRequest(p.request)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !bytes.Equal(response, []byte(`{"lbrycrd_address":"`+LbrycrdAddress+`","available_blobs":[]}`)) {
			t.Errorf("Response did not match expected response. Got %s", string(response))
		}
	}
}

func TestAvailabilityRequest_WithBlobs(t *testing.T) {
	s := getServer(t, true)

	for _, p := range availabilityRequests {
		response, err := s.handleAvailabilityRequest(p.request)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
		if !bytes.Equal(response, p.response) {
			t.Errorf("Response did not match expected response.\nExpected: %s\nGot: %s", string(p.response), string(response))
		}
	}
}

func TestRequestFromConnection(t *testing.T) {
	s := getServer(t, true)
	err := s.Start("127.0.0.1:50505")
	defer s.Shutdown()
	if err != nil {
		t.Error("error starting server", err)
	}

	for _, p := range availabilityRequests {
		conn, err := net.Dial("tcp", "127.0.0.1:50505")
		if err != nil {
			t.Error("error opening connection", err)
		}
		defer conn.Close()

		response := make([]byte, 8192)
		_, err = conn.Write(p.request)
		if err != nil {
			t.Error("error writing", err)
		}
		_, err = conn.Read(response)
		if err != nil {
			t.Error("error reading", err)
		}
		if !bytes.Equal(response[:len(p.response)], p.response) {
			t.Errorf("Response did not match expected response.\nExpected: %s\nGot: %s", string(p.response), string(response))
		}
	}
}

func TestInvalidData(t *testing.T) {
	s := getServer(t, true)
	err := s.Start("127.0.0.1:50503")
	defer s.Shutdown()
	if err != nil {
		t.Error("error starting server", err)
	}
	conn, err := net.Dial("tcp", "127.0.0.1:50503")
	if err != nil {
		t.Error("error opening connection", err)
	}
	defer conn.Close()

	response := make([]byte, 8192)
	_, err = conn.Write([]byte("hello dear server, I would like blobs. Please"))
	if err != nil {
		t.Error("error writing", err)
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Read(response)
	if err != io.EOF {
		t.Error("error reading", err)
	}
	println(response)
}
