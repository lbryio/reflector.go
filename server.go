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
	var sendRequest sendBlobRequest
	dec := json.NewDecoder(conn)
	err := dec.Decode(&sendRequest)
	if err != nil {
		return err
	} else if sendRequest.BlobSize > BlobSize {
		return fmt.Errorf("Blob size cannot be greater than " + strconv.Itoa(BlobSize) + " bytes")
	}

	// check if blob exists
	haveBlob := false
	sendResponse, err := json.Marshal(sendBlobResponse{SendBlob: !haveBlob})
	if err != nil {
		return err
	}

	_, err = conn.Write(sendResponse)
	if err != nil {
		return err
	}

	blob := make([]byte, sendRequest.BlobSize)
	_, err = io.ReadFull(bufio.NewReader(conn), blob)
	if err != nil {
		return err
	}

	blobHash := getBlobHash(blob)
	if sendRequest.BlobHash != blobHash {
		return fmt.Errorf("Hash of received blob does not match hash from send request")
	}
	log.Println("Got blob " + blobHash[:8])

	err = ioutil.WriteFile(path.Join(s.BlobDir, blobHash), blob, 0644)
	if err != nil {
		return err
	}

	transferResponse, err := json.Marshal(blobTransferResponse{ReceivedBlob: true})
	if err != nil {
		return err
	}

	_, err = conn.Write(transferResponse)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) doHandshake(conn net.Conn) error {
	var handshake handshakeRequestResponse
	dec := json.NewDecoder(conn)
	err := dec.Decode(&handshake)
	if err != nil {
		return err
	} else if handshake.Version != protocolVersion1 {
		return fmt.Errorf("This server only supports protocol version 1")
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
