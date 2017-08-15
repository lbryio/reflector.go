package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"strconv"
)

type Server struct {
	BlobDir string
}

func NewServer(blobDir string) *Server {
	return &Server{
		BlobDir: blobDir,
	}
}

func (s *Server) ListenAndServe(address string) error {
	log.Println("Blobs will be saved to " + s.BlobDir)
	err := s.ensureBlobDirExists()
	if err != nil {
		return err
	}

	log.Println("Listening on " + address)
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			// TODO: dont crash server on error here
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	// TODO: connection should time out eventually
	defer conn.Close()

	err := s.doHandshake(conn)
	if err != nil {
		if err == io.EOF {
			return
		}
		s.doError(conn, err)
		return
	}

	for {
		err = s.receiveBlob(conn)
		if err != nil {
			if err == io.EOF {
				return
			}
			s.doError(conn, err)
			return
		}

	}
}

func (s *Server) doError(conn net.Conn, e error) error {
	log.Println("Error: " + e.Error())
	if e2, ok := e.(*json.SyntaxError); ok {
		log.Printf("syntax error at byte offset %d", e2.Offset)
	}
	resp, err := json.Marshal(errorResponse{Error: e.Error()})
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
	blobPath := path.Join(s.BlobDir, blobHash)
	if !isSdBlob { // we have to say sd blobs are missing because if we say we have it, they wont try to send any content blobs
		if _, err := os.Stat(blobPath); !os.IsNotExist(err) {
			blobExists = true
		}
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
		return fmt.Errorf("Hash of received blob data does not match hash from send request")
		// this can also happen if the blob size is wrong, because the server will read the wrong number of bytes from the stream
	}
	log.Println("Got blob " + blobHash[:8])

	err = ioutil.WriteFile(blobPath, blob, 0644)
	if err != nil {
		return err
	}

	return s.sendTransferResponse(conn, true, isSdBlob)
}

func (s *Server) doHandshake(conn net.Conn) error {
	var handshake handshakeRequestResponse
	dec := json.NewDecoder(conn)
	err := dec.Decode(&handshake)
	if err != nil {
		return err
	} else if handshake.Version != protocolVersion1 && handshake.Version != protocolVersion2 {
		return fmt.Errorf("Protocol version not supported")
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
	dec := json.NewDecoder(conn)
	err := dec.Decode(&sendRequest)
	if err != nil {
		return 0, "", false, err
	}

	if sendRequest.SdBlobHash != "" && sendRequest.BlobHash != "" {
		return 0, "", false, fmt.Errorf("Invalid request")
	}

	var blobHash string
	var blobSize int
	isSdBlob := sendRequest.SdBlobHash != ""

	if isSdBlob {
		blobSize = sendRequest.SdBlobSize
		blobHash = sendRequest.SdBlobHash
		if blobSize > BlobSize {
			return 0, "", isSdBlob, fmt.Errorf("SD blob cannot be more than " + strconv.Itoa(BlobSize) + " bytes")
		}
	} else {
		blobSize = sendRequest.BlobSize
		blobHash = sendRequest.BlobHash
		if blobSize != BlobSize {
			return 0, "", isSdBlob, fmt.Errorf("Blob must be exactly " + strconv.Itoa(BlobSize) + " bytes")
		}
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

func (s *Server) ensureBlobDirExists() error {
	if stat, err := os.Stat(s.BlobDir); err != nil {
		if os.IsNotExist(err) {
			err2 := os.Mkdir(s.BlobDir, 0755)
			if err2 != nil {
				return err2
			}
		} else {
			return err
		}
	} else if !stat.IsDir() {
		return fmt.Errorf("blob dir exists but is not a dir")
	}
	return nil
}
