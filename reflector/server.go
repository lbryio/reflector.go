package reflector

import (
	"bufio"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"
	"github.com/lbryio/lbry.go/v2/stream"

	log "github.com/sirupsen/logrus"
)

const (
	// DefaultPort is the port the reflector server listens on if not passed in.
	DefaultPort = 5566
	// DefaultTimeout is the default timeout to read or write the next message
	DefaultTimeout = 5 * time.Second

	network          = "tcp4"
	protocolVersion1 = 0
	protocolVersion2 = 1
	maxBlobSize      = stream.MaxBlobSize
)

var ErrBlobTooBig = errors.Base("blob must be at most %d bytes", maxBlobSize)

// Server is and instance of the reflector server. It houses the blob store and listener.
type Server struct {
	Timeout time.Duration // timeout to read or write next message

	EnableBlocklist bool // if true, blocklist checking and blob deletion will be enabled

	//underlyingStore store.BlobStore
	//outerStore      store.BlobStore
	store store.BlobStore
	grp   *stop.Group
}

// NewServer returns an initialized reflector server pointer.
func NewServer(underlying store.BlobStore, outer store.BlobStore) *Server {
	return &Server{
		Timeout: DefaultTimeout,
		//underlyingStore: underlying,
		//outerStore:      outer,
		grp: stop.New(),
	}
}
func NewIngestionServer(store store.BlobStore) *Server {
	return &Server{
		Timeout: DefaultTimeout,
		store:   store,
		grp:     stop.New(),
	}
}

// Shutdown shuts down the reflector server gracefully.
func (s *Server) Shutdown() {
	log.Println("shutting down reflector server...")
	s.grp.StopAndWait()
	log.Println("reflector server stopped")
}

// Start starts the server to handle connections.
func (s *Server) Start(address string) error {
	l, err := net.Listen(network, address)
	if err != nil {
		return errors.Err(err)
	}
	log.Println("reflector listening on " + address)
	s.grp.Add(1)
	metrics.RoutinesQueue.WithLabelValues("reflector", "listener").Inc()
	go func() {
		defer metrics.RoutinesQueue.WithLabelValues("reflector", "listener").Dec()
		<-s.grp.Ch()
		err := l.Close()
		if err != nil {
			log.Error(errors.Prefix("closing listener", err))
		}
		s.grp.Done()
	}()

	s.grp.Add(1)
	metrics.RoutinesQueue.WithLabelValues("reflector", "start").Inc()
	go func() {
		defer metrics.RoutinesQueue.WithLabelValues("reflector", "start").Dec()
		s.listenAndServe(l)
		s.grp.Done()
	}()

	if s.EnableBlocklist {
		if b, ok := s.store.(store.Blocklister); ok {
			s.grp.Add(1)
			metrics.RoutinesQueue.WithLabelValues("reflector", "enableblocklist").Inc()
			go func() {
				defer metrics.RoutinesQueue.WithLabelValues("reflector", "enableblocklist").Dec()
				s.enableBlocklist(b)
				s.grp.Done()
			}()
		} else {
			//s.Shutdown()
			return errors.Err("blocklist is enabled but blob store does not support blocklisting")
		}
	}

	return nil
}

func (s *Server) listenAndServe(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.quitting() {
				return
			}
			log.Error(err)
		} else {
			s.grp.Add(1)
			metrics.RoutinesQueue.WithLabelValues("reflector", "server-listenandserve").Inc()
			go func() {
				defer metrics.RoutinesQueue.WithLabelValues("reflector", "server-listenandserve").Dec()
				s.handleConn(conn)
				s.grp.Done()
			}()
		}
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// all this stuff is to close the connections correctly when we're shutting down the server
	connNeedsClosing := make(chan struct{})
	defer func() {
		close(connNeedsClosing)
	}()
	s.grp.Add(1)
	metrics.RoutinesQueue.WithLabelValues("reflector", "server-handleconn").Inc()
	go func() {
		defer metrics.RoutinesQueue.WithLabelValues("reflector", "server-handleconn").Dec()
		defer s.grp.Done()
		select {
		case <-connNeedsClosing:
		case <-s.grp.Ch():
		}
		err := conn.Close()
		if err != nil {
			log.Error(errors.Prefix("closing peer conn", err))
		}
	}()

	err := s.doHandshake(conn)
	if err != nil {
		if errors.Is(err, io.EOF) || s.quitting() {
			return
		}
		err := s.doError(conn, err)
		if err != nil {
			log.Error(errors.Prefix("sending handshake error", err))
		}
		return
	}

	for {
		err = s.receiveBlob(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || s.quitting() {
				return
			}
			err := s.doError(conn, err)
			if err != nil {
				log.Error(errors.Prefix("sending blob receive error", err))
			}
			return
		}
	}
}

func (s *Server) doError(conn net.Conn, err error) error {
	if err == nil {
		return nil
	}
	shouldLog := metrics.TrackError(metrics.DirectionUpload, err)
	if shouldLog {
		log.Errorln(errors.FullTrace(err))
	}
	if e2, ok := err.(*json.SyntaxError); ok {
		log.Errorf("syntax error at byte offset %d", e2.Offset)
	}
	//resp, err := json.Marshal(errorResponse{Error: err.Error()})
	//if err != nil {
	//	return err
	//}
	//return s.write(conn, resp)
	return nil
}

func (s *Server) receiveBlob(conn net.Conn) error {
	blobSize, blobHash, isSdBlob, err := s.readBlobRequest(conn)
	if err != nil {
		return err
	}

	var wantsBlob bool
	if bl, ok := s.store.(store.Blocklister); ok {
		wantsBlob, err = bl.Wants(blobHash)
		if err != nil {
			return err
		}
	} else {
		blobExists, err := s.store.Has(blobHash)
		if err != nil {
			return err
		}
		wantsBlob = !blobExists
	}

	var neededBlobs []string

	if isSdBlob && !wantsBlob {
		if nbc, ok := s.store.(store.NeededBlobChecker); ok {
			neededBlobs, err = nbc.MissingBlobsForKnownStream(blobHash)
			if err != nil {
				return err
			}
		} else {
			// if we can't check for blobs in a stream, we have to say that the sd blob is
			// missing. if we say we have the sd blob, they won't try to send any content blobs
			wantsBlob = true
		}
	}

	err = s.sendBlobResponse(conn, wantsBlob, isSdBlob, neededBlobs)
	if err != nil {
		return err
	}

	if !wantsBlob {
		return nil
	}

	blob, err := s.readRawBlob(conn, blobSize)
	if err != nil {
		sendErr := s.sendTransferResponse(conn, false, isSdBlob)
		if sendErr != nil {
			return sendErr
		}
		return errors.Prefix("error reading blob "+blobHash[:8], err)
	}

	receivedBlobHash := BlobHash(blob)
	if blobHash != receivedBlobHash {
		sendErr := s.sendTransferResponse(conn, false, isSdBlob)
		if sendErr != nil {
			return sendErr
		}
		return errors.Err("hash of received blob data does not match hash from send request")
		// this can also happen if the blob size is wrong, because the server will read the wrong number of bytes from the stream
	}

	log.Debugln("Got blob " + blobHash[:8])

	if isSdBlob {
		err = s.store.PutSD(blobHash, blob)
	} else {
		err = s.store.Put(blobHash, blob)
	}
	if err != nil {
		return err
	}
	metrics.MtrInBytesReflector.Add(float64(len(blob)))
	metrics.BlobUploadCount.Inc()
	if isSdBlob {
		metrics.SDBlobUploadCount.Inc()
	}
	return s.sendTransferResponse(conn, true, isSdBlob)
}

func (s *Server) doHandshake(conn net.Conn) error {
	var handshake handshakeRequestResponse
	err := s.read(conn, &handshake)
	if err != nil {
		return err
	} else if handshake.Version == nil {
		return errors.Err("handshake is missing protocol version")
	} else if *handshake.Version != protocolVersion1 && *handshake.Version != protocolVersion2 {
		return errors.Err("protocol version not supported")
	}

	resp, err := json.Marshal(handshakeRequestResponse{Version: handshake.Version})
	if err != nil {
		return err
	}

	return s.write(conn, resp)
}

func (s *Server) readBlobRequest(conn net.Conn) (int, string, bool, error) {
	var sendRequest sendBlobRequest
	err := s.read(conn, &sendRequest)
	if err != nil {
		return 0, "", false, err
	}

	var blobHash string
	var blobSize int
	isSdBlob := sendRequest.SdBlobHash != ""

	if isSdBlob {
		blobSize = sendRequest.SdBlobSize
		blobHash = sendRequest.SdBlobHash
	} else {
		blobSize = sendRequest.BlobSize
		blobHash = sendRequest.BlobHash
	}

	if blobHash == "" {
		return blobSize, blobHash, isSdBlob, errors.Err("blob hash is empty")
	}
	if blobSize > maxBlobSize {
		return blobSize, blobHash, isSdBlob, errors.Err(ErrBlobTooBig)
	}
	if blobSize == 0 {
		return blobSize, blobHash, isSdBlob, errors.Err("0-byte blob received")
	}

	return blobSize, blobHash, isSdBlob, nil
}

func (s *Server) sendBlobResponse(conn net.Conn, shouldSendBlob, isSdBlob bool, neededBlobs []string) error {
	var response []byte
	var err error

	if isSdBlob {
		response, err = json.Marshal(sendSdBlobResponse{SendSdBlob: shouldSendBlob, NeededBlobs: neededBlobs})
	} else {
		response, err = json.Marshal(sendBlobResponse{SendBlob: shouldSendBlob})
	}
	if err != nil {
		return err
	}

	return s.write(conn, response)
}

func (s *Server) sendTransferResponse(conn net.Conn, receivedBlob, isSdBlob bool) error {
	var response []byte
	var err error

	if isSdBlob {
		response, err = json.Marshal(sdBlobTransferResponse{ReceivedSdBlob: receivedBlob})
	} else {
		response, err = json.Marshal(blobTransferResponse{ReceivedBlob: receivedBlob})
	}
	if err != nil {
		return err
	}

	return s.write(conn, response)
}

func (s *Server) read(conn net.Conn, v interface{}) error {
	err := conn.SetReadDeadline(time.Now().Add(s.Timeout))
	if err != nil {
		return errors.Err(err)
	}

	dec := json.NewDecoder(conn)
	err = dec.Decode(v)
	if err != nil {
		data, _ := io.ReadAll(dec.Buffered())
		if len(data) > 0 {
			return errors.Err("%s. Data: %s", err.Error(), hex.EncodeToString(data))
		}
		return errors.Err(err)
	}
	return nil
}

func (s *Server) readRawBlob(conn net.Conn, blobSize int) ([]byte, error) {
	err := conn.SetReadDeadline(time.Now().Add(s.Timeout))
	if err != nil {
		return nil, errors.Err(err)
	}

	blob := make([]byte, blobSize)
	_, err = io.ReadFull(bufio.NewReader(conn), blob)
	return blob, errors.Err(err)
}

func (s *Server) write(conn net.Conn, b []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(s.Timeout))
	if err != nil {
		return errors.Err(err)
	}

	n, err := conn.Write(b)
	if err == nil && n != len(b) {
		err = io.ErrShortWrite
	}
	return errors.Err(err)
}

func (s *Server) quitting() bool {
	select {
	case <-s.grp.Ch():
		return true
	default:
		return false
	}
}

// BlobHash returns the sha512 hash hex encoded string of the blob byte slice.
func BlobHash(blob []byte) string {
	hashBytes := sha512.Sum384(blob)
	return hex.EncodeToString(hashBytes[:])
}

func IsValidJSON(b []byte) bool {
	var r json.RawMessage
	return json.Unmarshal(b, &r) == nil
}

//type errorResponse struct {
//	Error string `json:"error"`
//}

type handshakeRequestResponse struct {
	Version *int `json:"version"`
}

type sendBlobRequest struct {
	BlobHash   string `json:"blob_hash,omitempty"`
	BlobSize   int    `json:"blob_size,omitempty"`
	SdBlobHash string `json:"sd_blob_hash,omitempty"`
	SdBlobSize int    `json:"sd_blob_size,omitempty"`
}

type sendBlobResponse struct {
	SendBlob bool `json:"send_blob"`
}

type sendSdBlobResponse struct {
	SendSdBlob  bool     `json:"send_sd_blob"`
	NeededBlobs []string `json:"needed_blobs,omitempty"`
}

type blobTransferResponse struct {
	ReceivedBlob bool `json:"received_blob"`
}

type sdBlobTransferResponse struct {
	ReceivedSdBlob bool `json:"received_sd_blob"`
}
