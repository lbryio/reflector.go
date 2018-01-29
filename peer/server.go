package peer

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"

	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
)

const (
	DefaultPort = 3333
)

type Server struct {
	store store.BlobStore
}

func NewServer(store store.BlobStore) *Server {
	return &Server{
		store: store,
	}
}

func (s *Server) ListenAndServe(address string) error {
	log.Println("Listening on " + address)
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Error(err)
		} else {
			go s.handleConn(conn)
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// TODO: connection should time out eventually
	defer conn.Close()

	err := s.doAvailabilityRequest(conn)
	if err != nil {
		log.Error(err)
		return
	}

	err = s.doPaymentRateNegotiation(conn)
	if err != nil {
		log.Error(err)
		return
	}

	for {
		err = s.doBlobRequest(conn)
		if err != nil {
			if err != io.EOF {
				log.Error(err)
			}
			return
		}
	}
}

func (s *Server) doAvailabilityRequest(conn net.Conn) error {
	var request availabilityRequest
	err := json.NewDecoder(conn).Decode(&request)
	if err != nil {
		return err
	}

	address := "bJxKvpD96kaJLriqVajZ7SaQTsWWyrGQct"
	availableBlobs := []string{}
	for _, blobHash := range request.RequestedBlobs {
		exists, err := s.store.Has(blobHash)
		if err != nil {
			return err
		}
		if exists {
			availableBlobs = append(availableBlobs, blobHash)
		}
	}

	response, err := json.Marshal(availabilityResponse{LbrycrdAddress: address, AvailableBlobs: availableBlobs})
	if err != nil {
		return err
	}

	_, err = conn.Write(response)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) doPaymentRateNegotiation(conn net.Conn) error {
	var request paymentRateRequest
	err := json.NewDecoder(conn).Decode(&request)
	if err != nil {
		return err
	}

	offerReply := paymentRateAccepted
	if request.BlobDataPaymentRate < 0 {
		offerReply = paymentRateTooLow
	}

	response, err := json.Marshal(paymentRateResponse{BlobDataPaymentRate: offerReply})
	if err != nil {
		return err
	}

	_, err = conn.Write(response)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) doBlobRequest(conn net.Conn) error {
	var request blobRequest
	err := json.NewDecoder(conn).Decode(&request)
	if err != nil {
		return err
	}

	log.Println("Sending blob " + request.RequestedBlob[:8])

	blob, err := s.store.Get(request.RequestedBlob)
	if err != nil {
		return err
	}

	response, err := json.Marshal(blobResponse{IncomingBlob: incomingBlob{
		BlobHash: getBlobHash(blob),
		Length:   len(blob),
	}})
	if err != nil {
		return err
	}

	_, err = conn.Write(response)
	if err != nil {
		return err
	}

	_, err = conn.Write(blob)
	if err != nil {
		return err
	}

	return nil
}

func readAll(conn net.Conn) {
	buf := make([]byte, 0, 4096) // big buffer
	tmp := make([]byte, 256)     // using small tmo buffer for demonstrating
	for {
		n, err := conn.Read(tmp)
		if err != nil {
			if err != io.EOF {
				log.Println("read error:", err)
			}
			break
		}
		log.Println("got", n, "bytes.")
		buf = append(buf, tmp[:n]...)
	}
	log.Println("total size:", len(buf))
	if len(buf) > 0 {
		log.Println(string(buf))
	}
}

func getBlobHash(blob []byte) string {
	hashBytes := sha512.Sum384(blob)
	return hex.EncodeToString(hashBytes[:])
}
