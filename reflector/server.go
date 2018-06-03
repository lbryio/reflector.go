package reflector

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"strconv"

	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/errors"

	log "github.com/sirupsen/logrus"
)

// Server is and instance of the reflector server. It houses the blob store and listener.
type Server struct {
	store  store.BlobStore
	l      net.Listener
	closed bool
}

// NewServer returns an initialized reflector server pointer.
func NewServer(store store.BlobStore) *Server {
	return &Server{
		store: store,
	}
}

// Shutdown shuts down the reflector server gracefully.
func (s *Server) Shutdown() {
	// TODO: need waitgroup so we can finish whatever we're doing before stopping
	s.closed = true
	if err := s.l.Close(); err != nil {
		log.Error("error shutting down reflector server - ", err)
	}
}

func closeListener(listener net.Listener) {
	if err := listener.Close(); err != nil {
		log.Error("error closing reflector server listener - ", err)
	}
}

//ListenAndServe starts the server listener to handle connections.
func (s *Server) ListenAndServe(address string) error {
	//ToDo - We should make this DRY as it is the same code in both servers.
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
			go s.handleConn(conn)
		}
	}
}

func closeConnection(conn net.Conn) {
	if err := conn.Close(); err != nil {
		log.Error("error closing reflector client connection - ", err)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// TODO: connection should time out eventually
	defer closeConnection(conn)

	err := s.doHandshake(conn)
	if err != nil {
		if err == io.EOF {
			return
		}
		if err := s.doError(conn, err); err != nil {
			log.Error("error sending error response to reflector client connection - ", err)
		}
		return
	}

	for {
		err = s.receiveBlob(conn)
		if err != nil {
			if err != io.EOF {
				if err := s.doError(conn, err); err != nil {
					log.Error("error sending error response for receiving a blob to reflector client connection - ", err)
				}
			}
			return
		}
	}
}

func (s *Server) doError(conn net.Conn, err error) error {
	log.Errorln(err)
	if e2, ok := err.(*json.SyntaxError); ok {
		log.Printf("syntax error at byte offset %d", e2.Offset)
	}
	resp, err := json.Marshal(errorResponse{Error: err.Error()})
	if err != nil {
		return err
	}
	_, err = conn.Write(resp)
	return err
}

func (s *Server) receiveBlob(conn net.Conn) error {
	blobSize, blobHash, isSdBlob, err := s.readBlobRequest(conn)
	if err != nil {
		return err
	}

	blobExists := false
	if !isSdBlob {
		// we have to say sd blobs are missing because if we say we have it, they wont try to send any content blobs
		has, err := s.store.Has(blobHash)
		if err != nil {
			return err
		}
		blobExists = has
	}

	err = s.sendBlobResponse(conn, blobExists, isSdBlob)
	if err != nil {
		return err
	}

	if blobExists {
		return nil
	}

	blob := make([]byte, blobSize)
	_, err = io.ReadFull(bufio.NewReader(conn), blob)
	if err != nil {
		return err
	}

	receivedBlobHash := getBlobHash(blob)
	if blobHash != receivedBlobHash {
		return errors.Err("hash of received blob data does not match hash from send request")
		// this can also happen if the blob size is wrong, because the server will read the wrong number of bytes from the stream
	}

	log.Println("Got blob " + blobHash[:8])

	if isSdBlob {
		err = s.store.PutSD(blobHash, blob)
	} else {
		err = s.store.Put(blobHash, blob)
	}
	if err != nil {
		return err
	}

	return s.sendTransferResponse(conn, true, isSdBlob)
}

func (s *Server) doHandshake(conn net.Conn) error {
	var handshake handshakeRequestResponse
	err := json.NewDecoder(conn).Decode(&handshake)
	if err != nil {
		return err
	} else if handshake.Version != protocolVersion1 && handshake.Version != protocolVersion2 {
		return errors.Err("protocol version not supported")
	}

	resp, err := json.Marshal(handshakeRequestResponse{Version: handshake.Version})
	if err != nil {
		return err
	}

	_, err = conn.Write(resp)
	if err != nil {
		return err
	}

	return nil
}

func (s *Server) readBlobRequest(conn net.Conn) (int, string, bool, error) {
	var sendRequest sendBlobRequest
	err := json.NewDecoder(conn).Decode(&sendRequest)
	if err != nil {
		return 0, "", false, err
	}

	if sendRequest.SdBlobHash != "" && sendRequest.BlobHash != "" {
		return 0, "", false, errors.Err("invalid request")
	}

	var blobHash string
	var blobSize int
	isSdBlob := sendRequest.SdBlobHash != ""

	if blobSize > maxBlobSize {
		return 0, "", isSdBlob, errors.Err("blob cannot be more than " + strconv.Itoa(maxBlobSize) + " bytes")
	}

	if isSdBlob {
		blobSize = sendRequest.SdBlobSize
		blobHash = sendRequest.SdBlobHash
	} else {
		blobSize = sendRequest.BlobSize
		blobHash = sendRequest.BlobHash
	}

	return blobSize, blobHash, isSdBlob, nil
}

func (s *Server) sendBlobResponse(conn net.Conn, blobExists, isSdBlob bool) error {
	var response []byte
	var err error

	if isSdBlob {
		response, err = json.Marshal(sendSdBlobResponse{SendSdBlob: !blobExists})
	} else {
		response, err = json.Marshal(sendBlobResponse{SendBlob: !blobExists})
	}
	if err != nil {
		return err
	}

	_, err = conn.Write(response)
	if err != nil {
		return err
	}
	return nil
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

	_, err = conn.Write(response)
	if err != nil {
		return err
	}
	return nil
}
