package peer

import (
	"bufio"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/reflector.go/store"

	"github.com/davecgh/go-spew/spew"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultPort    = 3333
	LbrycrdAddress = "bJxKvpD96kaJLriqVajZ7SaQTsWWyrGQct"
)

type Server struct {
	store  store.BlobStore
	l      net.Listener
	closed bool
}

func NewServer(store store.BlobStore) *Server {
	return &Server{
		store: store,
	}
}

func (s *Server) Shutdown() {
	// TODO: need waitgroup so we can finish whatever we're doing before stopping
	s.closed = true
	if err := s.l.Close(); err != nil {
		log.Error("error shuting down peer server - ", err)
	}
}

func closeListener(listener net.Listener) {
	if err := listener.Close(); err != nil {
		log.Error("error closing listener for peer server - ", err)
	}
}

func (s *Server) ListenAndServe(address string) error {
	log.Println("Listening on " + address)
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer closeListener(l)

	for {
		conn, err := l.Accept()
		if err != nil {
			if s.closed {
				return nil
			}
			log.Error(err)
		} else {
			go s.handleConnection(conn)
		}
	}
}

func closeConnection(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Error("error closing client connection for peer server - ", err)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer closeConnection(conn)

	timeoutDuration := 5 * time.Second

	for {
		var request []byte
		var response []byte
		var err error

		if err := conn.SetReadDeadline(time.Now().Add(timeoutDuration)); err != nil {
			log.Error("error setting read deadline for client connection - ", err)
		}
		request, err = readNextRequest(conn)
		if err != nil {
			if err != io.EOF {
				log.Errorln(err)
			}
			return
		}
		if err := conn.SetReadDeadline(time.Time{}); err != nil {
			log.Error("error setting read deadline client connection - ", err)
		}

		if strings.Contains(string(request), `"requested_blobs"`) {
			log.Debugln("received availability request")
			response, err = s.handleAvailabilityRequest(request)
		} else if strings.Contains(string(request), `"blob_data_payment_rate"`) {
			log.Debugln("received rate negotiation request")
			response, err = s.handlePaymentRateNegotiation(request)
		} else if strings.Contains(string(request), `"requested_blob"`) {
			log.Debugln("received blob request")
			response, err = s.handleBlobRequest(request)
		} else {
			log.Errorln("invalid request")
			spew.Dump(request)
			return
		}
		if err != nil {
			log.Error(err)
			return
		}

		n, err := conn.Write(response)
		if err != nil {
			log.Errorln(err)
			return
		} else if n != len(response) {
			log.Errorln(io.ErrShortWrite)
			return
		}
	}
}

func (s *Server) handleAvailabilityRequest(data []byte) ([]byte, error) {
	var request availabilityRequest
	err := json.Unmarshal(data, &request)
	if err != nil {
		return []byte{}, err
	}

	availableBlobs := []string{}
	for _, blobHash := range request.RequestedBlobs {
		exists, err := s.store.Has(blobHash)
		if err != nil {
			return []byte{}, err
		}
		if exists {
			availableBlobs = append(availableBlobs, blobHash)
		}
	}

	return json.Marshal(availabilityResponse{LbrycrdAddress: LbrycrdAddress, AvailableBlobs: availableBlobs})
}

func (s *Server) handlePaymentRateNegotiation(data []byte) ([]byte, error) {
	var request paymentRateRequest
	err := json.Unmarshal(data, &request)
	if err != nil {
		return []byte{}, err
	}

	offerReply := paymentRateAccepted
	if request.BlobDataPaymentRate < 0 {
		offerReply = paymentRateTooLow
	}

	return json.Marshal(paymentRateResponse{BlobDataPaymentRate: offerReply})
}

func (s *Server) handleBlobRequest(data []byte) ([]byte, error) {
	var request blobRequest
	err := json.Unmarshal(data, &request)
	if err != nil {
		return []byte{}, err
	}

	log.Println("Sending blob " + request.RequestedBlob[:8])

	blob, err := s.store.Get(request.RequestedBlob)
	if err != nil {
		return []byte{}, err
	}

	response, err := json.Marshal(blobResponse{IncomingBlob: incomingBlob{
		BlobHash: GetBlobHash(blob),
		Length:   len(blob),
	}})
	if err != nil {
		return []byte{}, err
	}

	return append(response, blob...), nil
}

func readNextRequest(conn net.Conn) ([]byte, error) {
	request := make([]byte, 0)
	eof := false
	buf := bufio.NewReader(conn)

	for {
		chunk, err := buf.ReadBytes('}')
		if err != nil {
			if err != io.EOF {
				log.Errorln("read error:", err)
				return request, err
			}
			eof = true
		}

		//log.Debugln("got", len(chunk), "bytes.")
		//spew.Dump(chunk)

		if len(chunk) > 0 {
			request = append(request, chunk...)

			if len(request) > maxRequestSize {
				return request, errRequestTooLarge
			}

			// yes, this is how the peer protocol knows when the request finishes
			if isValidJSON(request) {
				break
			}
		}

		if eof {
			break
		}
	}

	//log.Debugln("total size:", len(request))
	//if len(request) > 0 {
	//	spew.Dump(request)
	//}

	if len(request) == 0 && eof {
		return []byte{}, io.EOF
	}

	return request, nil
}

func isValidJSON(b []byte) bool {
	var r json.RawMessage
	return json.Unmarshal(b, &r) == nil
}

func GetBlobHash(blob []byte) string {
	hashBytes := sha512.Sum384(blob)
	return hex.EncodeToString(hashBytes[:])
}

const (
	maxRequestSize      = 64 * (2 ^ 10) // 64kb
	paymentRateAccepted = "RATE_ACCEPTED"
	paymentRateTooLow   = "RATE_TOO_LOW"
	//ToDo: paymentRateUnset is not used, can we remove?
	//paymentRateUnset    = "RATE_UNSET"
)

var errRequestTooLarge = errors.Base("request is too large")

type availabilityRequest struct {
	LbrycrdAddress bool     `json:"lbrycrd_address"`
	RequestedBlobs []string `json:"requested_blobs"`
}

type availabilityResponse struct {
	LbrycrdAddress string   `json:"lbrycrd_address"`
	AvailableBlobs []string `json:"available_blobs"`
}

type paymentRateRequest struct {
	BlobDataPaymentRate float64 `json:"blob_data_payment_rate"`
}

type paymentRateResponse struct {
	BlobDataPaymentRate string `json:"blob_data_payment_rate"`
}

type blobRequest struct {
	RequestedBlob string `json:"requested_blob"`
}

type incomingBlob struct {
	Error    string `json:"error,omitempty"`
	BlobHash string `json:"blob_hash"`
	Length   int    `json:"length"`
}
type blobResponse struct {
	IncomingBlob incomingBlob `json:"incoming_blob"`
}
