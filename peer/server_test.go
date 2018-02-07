package peer

import (
	"bytes"
	"testing"

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

func getServer(withBlobs bool) *Server {
	st := store.MemoryBlobStore{}
	if withBlobs {
		for k, v := range blobs {
			st.Put(k, v)
		}
	}
	return NewServer(&st)
}

func TestAvailabilityRequest_NoBlobs(t *testing.T) {
	s := getServer(false)

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
	s := getServer(true)

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
