package peer

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strings"
	"time"

	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

const (
	// DefaultPort is the port the peer server listens on if not passed in.
	DefaultPort = 3333
	// LbrycrdAddress to be used when paying for data. Not implemented yet.
	LbrycrdAddress = "bJxKvpD96kaJLriqVajZ7SaQTsWWyrGQct"
)

// Server is an instance of a peer server that houses the listener and store.
type Server struct {
	StatLogger          *log.Logger   // logger to log stats
	StatReportFrequency time.Duration // how often to log stats

	store  store.BlobStore
	closed bool

	grp   *stop.Group
	stats *reflector.Stats
}

// NewServer returns an initialized Server pointer.
func NewServer(store store.BlobStore) *Server {
	return &Server{
		store: store,
		grp:   stop.New(),
	}
}

// Shutdown gracefully shuts down the peer server.
func (s *Server) Shutdown() {
	log.Debug("shutting down peer server...")
	s.stats.Shutdown()
	s.grp.StopAndWait()
	log.Debug("peer server stopped")
}

// Start starts the server listener to handle connections.
func (s *Server) Start(address string) error {
	log.Println("peer listening on " + address)
	l, err := net.Listen("tcp4", address)
	if err != nil {
		return err
	}

	go s.listenForShutdown(l)
	s.grp.Add(1)
	go func() {
		s.listenAndServe(l)
		s.grp.Done()
	}()

	s.stats = reflector.NewStatLogger("DOWNLOAD", s.StatLogger, s.StatReportFrequency, s.grp.Child())
	if s.StatLogger != nil && s.StatReportFrequency > 0 {
		s.stats.Start()
	}

	return nil
}

func (s *Server) listenForShutdown(listener net.Listener) {
	<-s.grp.Ch()
	s.closed = true
	err := listener.Close()
	if err != nil {
		log.Error("error closing listener for peer server - ", err)
	}
}

func (s *Server) listenAndServe(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.closed {
				return
			}
			log.Error(errors.Prefix("accepting conn", err))
		} else {
			s.grp.Add(1)
			go func() {
				s.handleConnection(conn)
				s.grp.Done()
			}()
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		if err := conn.Close(); err != nil {
			log.Error(errors.Prefix("closing peer conn", err))
		}
	}()

	timeoutDuration := 1 * time.Minute
	buf := bufio.NewReader(conn)

	for {
		var request []byte
		var response []byte

		err := conn.SetReadDeadline(time.Now().Add(timeoutDuration))
		if err != nil {
			log.Error(errors.FullTrace(err))
		}

		request, err = readNextMessage(buf)
		if err != nil {
			if err != io.EOF {
				s.logError(err)
			}
			return
		}

		err = conn.SetReadDeadline(time.Time{})
		if err != nil {
			log.Error(errors.FullTrace(err))
		}

		response, err = s.handleCompositeRequest(request)
		if err != nil {
			log.Error(err)
			return
		}

		err = conn.SetWriteDeadline(time.Now().Add(timeoutDuration))
		if err != nil {
			log.Error(errors.FullTrace(err))
		}

		n, err := conn.Write(response)
		if err != nil {
			if !strings.Contains(err.Error(), "connection reset by peer") { // means the other side closed the connection using TCP reset
				s.logError(err)
			}
			return
		} else if n != len(response) {
			log.Errorln(io.ErrShortWrite)
			return
		}

		err = conn.SetWriteDeadline(time.Time{})
		if err != nil {
			log.Error(errors.FullTrace(err))
		}
	}
}

func (s *Server) handleAvailabilityRequest(data []byte) ([]byte, error) {
	var request availabilityRequest
	err := json.Unmarshal(data, &request)
	if err != nil {
		return nil, err
	}

	availableBlobs := []string{}
	for _, blobHash := range request.RequestedBlobs {
		exists, err := s.store.Has(blobHash)
		if err != nil {
			return nil, err
		}
		if exists {
			availableBlobs = append(availableBlobs, blobHash)
		}
	}

	return json.Marshal(availabilityResponse{LbrycrdAddress: LbrycrdAddress, AvailableBlobs: availableBlobs})
}

//func (s *Server) handlePaymentRateNegotiation(data []byte) ([]byte, error) {
//	var request paymentRateRequest
//	err := json.Unmarshal(data, &request)
//	if err != nil {
//		return nil, err
//	}
//
//	offerReply := paymentRateAccepted
//	if request.BlobDataPaymentRate < 0 {
//		offerReply = paymentRateTooLow
//	}
//
//	return json.Marshal(paymentRateResponse{BlobDataPaymentRate: offerReply})
//}
//
//func (s *Server) handleBlobRequest(data []byte) ([]byte, error) {
//	var request blobRequest
//	err := json.Unmarshal(data, &request)
//	if err != nil {
//		return nil, err
//	}
//
//	log.Debugln("Sending blob " + request.RequestedBlob[:8])
//
//	blob, err := s.store.Get(request.RequestedBlob)
//	if err != nil {
//		return nil, err
//	}
//
//	response, err := json.Marshal(blobResponse{IncomingBlob: incomingBlob{
//		BlobHash: reflector.BlobHash(blob),
//		Length:   len(blob),
//	}})
//	if err != nil {
//		return nil, err
//	}
//
//	return append(response, blob...), nil
//}

func (s *Server) handleCompositeRequest(data []byte) ([]byte, error) {
	var request compositeRequest
	err := json.Unmarshal(data, &request)
	if err != nil {
		return nil, err
	}

	response := compositeResponse{
		LbrycrdAddress: LbrycrdAddress,
	}

	if len(request.RequestedBlobs) > 0 {
		var availableBlobs []string
		for _, blobHash := range request.RequestedBlobs {
			exists, err := s.store.Has(blobHash)
			if err != nil {
				return nil, err
			}
			if exists {
				availableBlobs = append(availableBlobs, blobHash)
			}
		}
		response.AvailableBlobs = availableBlobs
	}

	response.BlobDataPaymentRate = paymentRateAccepted
	if request.BlobDataPaymentRate < 0 {
		response.BlobDataPaymentRate = paymentRateTooLow
	}

	var blob []byte
	if request.RequestedBlob != "" {
		if len(request.RequestedBlob) != stream.BlobHashHexLength {
			return nil, errors.Err("Invalid blob hash length")
		}

		log.Debugln("Sending blob " + request.RequestedBlob[:8])

		blob, err = s.store.Get(request.RequestedBlob)
		if errors.Is(err, store.ErrBlobNotFound) {
			response.IncomingBlob = incomingBlob{
				Error: err.Error(),
			}
		} else if err != nil {
			return nil, err
		} else {
			response.IncomingBlob = incomingBlob{
				BlobHash: reflector.BlobHash(blob),
				Length:   len(blob),
			}
			s.stats.AddBlob()
		}
	}

	respData, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}

	return append(respData, blob...), nil
}

func (s *Server) logError(e error) {
	if e == nil {
		return
	}
	shouldLog := s.stats.AddError(e)
	if shouldLog {
		log.Errorln(errors.FullTrace(e))
	}

	return

	// old stuff below. its here for posterity, because we're gonna have to deal with these errors someday for real

	//err := errors.Wrap(e, 0)

	// these happen because the peer protocol does not have a way to cancel blob downloads
	// so the client will just close the connection if its in the middle of downloading a blob
	// but receives the blob from a different peer first or simply goes offline (timeout)
	//if strings.Contains(err.Error(), "connection reset by peer") ||
	//	strings.Contains(err.Error(), "i/o timeout") ||
	//	strings.Contains(err.Error(), "broken pipe") {
	//	return
	//}
	//
	//log.Error(errors.FullTrace(e))
}

func readNextMessage(buf *bufio.Reader) ([]byte, error) {
	msg := make([]byte, 0)
	eof := false

	for {
		chunk, err := buf.ReadBytes('}')
		if err != nil {
			if err != io.EOF {
				//log.Errorln("readBytes error:", err) // logged by caller
				return msg, err
			}
			eof = true
		}

		//log.Debugln("got", len(chunk), "bytes.")
		//spew.Dump(chunk)

		if len(chunk) > 0 {
			msg = append(msg, chunk...)

			if len(msg) > maxRequestSize {
				return msg, errRequestTooLarge
			}

			// yes, this is how the peer protocol knows when the request finishes
			if reflector.IsValidJSON(msg) {
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

	if len(msg) == 0 && eof {
		return nil, io.EOF
	}

	return msg, nil
}

const (
	maxRequestSize      = 64 * (2 ^ 10) // 64kb
	paymentRateAccepted = "RATE_ACCEPTED"
	paymentRateTooLow   = "RATE_TOO_LOW"
	//ToDo: paymentRateUnset is not used but exists in the protocol.
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

type compositeRequest struct {
	LbrycrdAddress      bool     `json:"lbrycrd_address"`
	RequestedBlobs      []string `json:"requested_blobs"`
	BlobDataPaymentRate float64  `json:"blob_data_payment_rate"`
	RequestedBlob       string   `json:"requested_blob"`
}

type compositeResponse struct {
	LbrycrdAddress      string       `json:"lbrycrd_address,omitempty"`
	AvailableBlobs      []string     `json:"available_blobs,omitempty"`
	BlobDataPaymentRate string       `json:"blob_data_payment_rate,omitempty"`
	IncomingBlob        incomingBlob `json:"incoming_blob,omitempty"`
}
