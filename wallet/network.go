package wallet

// copied from https://github.com/d4l3k/go-electrum

import (
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/lbryio/lbry.go/stop"
	log "github.com/sirupsen/logrus"
	"github.com/uber-go/atomic"
)

const (
	ClientVersion   = "0.0.1"
	ProtocolVersion = "1.0"
)

var (
	ErrNotImplemented = errors.New("not implemented")
	ErrNodeConnected  = errors.New("node already connected")
	ErrConnectFailed  = errors.New("failed to connect")
)

type response struct {
	Id     uint32 `json:"id"`
	Method string `json:"method"`
	Error  struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type request struct {
	Id     uint32   `json:"id"`
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type Node struct {
	transport *TCPTransport
	nextId    atomic.Uint32
	grp       *stop.Group

	handlersMu sync.RWMutex
	handlers   map[uint32]chan []byte

	pushHandlersMu sync.RWMutex
	pushHandlers   map[string][]chan []byte
}

// NewNode creates a new node.
func NewNode() *Node {
	return &Node{
		handlers:     make(map[uint32]chan []byte),
		pushHandlers: make(map[string][]chan []byte),
		grp:          stop.New(),
	}
}

// Connect creates a new connection to the specified address.
func (n *Node) Connect(addrs []string, config *tls.Config) error {
	if n.transport != nil {
		return ErrNodeConnected
	}

	// shuffle addresses for load balancing
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(addrs), func(i, j int) { addrs[i], addrs[j] = addrs[j], addrs[i] })

	var err error

	for _, addr := range addrs {
		n.transport, err = NewTransport(addr, config)
		if err == nil {
			break
		}
		if e, ok := err.(*net.OpError); ok && e.Err.Error() == "no such host" {
			// net.errNoSuchHost is not exported, so we have to string-match
			continue
		}
		return err
	}

	if n.transport == nil {
		return ErrConnectFailed
	}

	log.Debugf("wallet connected to %s", n.transport.conn.RemoteAddr())

	n.grp.Add(1)
	go func() {
		defer n.grp.Done()
		<-n.grp.Ch()
		n.transport.Shutdown()
	}()

	n.grp.Add(1)
	go func() {
		defer n.grp.Done()
		n.handleErrors()
	}()

	n.grp.Add(1)
	go func() {
		defer n.grp.Done()
		n.listen()
	}()

	return nil
}

func (n *Node) Shutdown() {
	n.grp.StopAndWait()
}

func (n *Node) handleErrors() {
	for {
		select {
		case <-n.grp.Ch():
			return
		case err := <-n.transport.Errors():
			n.err(err)
		}
	}
}

// err handles errors produced by the foreign node.
func (n *Node) err(err error) {
	// TODO: Better error handling.
	log.Error(err)
}

// listen processes messages from the server.
func (n *Node) listen() {
	for {
		select {
		case <-n.grp.Ch():
			return
		case bytes := <-n.transport.Responses():
			msg := &response{}
			if err := json.Unmarshal(bytes, msg); err != nil {
				n.err(err)
				continue
			}
			if len(msg.Error.Message) > 0 {
				n.err(fmt.Errorf("error from server: %#v", msg.Error.Message))
				continue
			}
			if len(msg.Method) > 0 {
				n.pushHandlersMu.RLock()
				handlers := n.pushHandlers[msg.Method]
				n.pushHandlersMu.RUnlock()

				for _, handler := range handlers {
					select {
					case handler <- bytes:
					default:
					}
				}
			}

			n.handlersMu.RLock()
			c, ok := n.handlers[msg.Id]
			n.handlersMu.RUnlock()
			if ok {
				c <- bytes
			}
		}
	}
}

// listenPush returns a channel of messages matching the method.
func (n *Node) listenPush(method string) <-chan []byte {
	c := make(chan []byte, 1)
	n.pushHandlersMu.Lock()
	defer n.pushHandlersMu.Unlock()
	n.pushHandlers[method] = append(n.pushHandlers[method], c)
	return c
}

// request makes a request to the server and unmarshals the response into v.
func (n *Node) request(method string, params []string, v interface{}) error {
	msg := request{
		Id:     n.nextId.Load(),
		Method: method,
		Params: params,
	}
	n.nextId.Inc()

	bytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	bytes = append(bytes, delimiter)

	err = n.transport.Send(bytes)
	if err != nil {
		return err
	}

	c := make(chan []byte, 1)

	n.handlersMu.Lock()
	n.handlers[msg.Id] = c
	n.handlersMu.Unlock()

	resp := <-c

	n.handlersMu.Lock()
	delete(n.handlers, msg.Id)
	n.handlersMu.Unlock()

	return json.Unmarshal(resp, v)
}
