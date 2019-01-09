package wallet

// copied from https://github.com/d4l3k/go-electrum

import (
	"bufio"
	"crypto/tls"
	"net"
	"time"

	"github.com/lbryio/lbry.go/extras/stop"
	log "github.com/sirupsen/logrus"
)

type TCPTransport struct {
	conn      net.Conn
	responses chan []byte
	errors    chan error
	grp       *stop.Group
}

func NewTransport(addr string, config *tls.Config) (*TCPTransport, error) {
	var conn net.Conn
	var err error

	timeout := 5 * time.Second
	if config != nil {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, config)
	} else {
		conn, err = net.DialTimeout("tcp", addr, timeout)
	}
	if err != nil {
		return nil, err
	}

	t := &TCPTransport{
		conn:      conn,
		responses: make(chan []byte),
		errors:    make(chan error),
		grp:       stop.New(),
	}

	t.grp.Add(1)
	go func() {
		defer t.grp.Done()
		<-t.grp.Ch()
		t.close()
	}()

	t.grp.Add(1)
	go func() {
		t.grp.Done()
		t.listen()
	}()

	return t, nil
}

const delimiter = byte('\n')

func (t *TCPTransport) listen() {
	reader := bufio.NewReader(t.conn)
	for {
		line, err := reader.ReadBytes(delimiter)
		if err != nil {
			t.error(err)
			return
		}

		log.Debugf("%s -> %s", t.conn.RemoteAddr(), line)

		t.responses <- line
	}
}

func (t *TCPTransport) Send(body []byte) error {
	log.Debugf("%s <- %s", t.conn.RemoteAddr(), body)
	_, err := t.conn.Write(body)
	return err
}

func (t *TCPTransport) error(err error) {
	select {
	case t.errors <- err:
	default:
	}
}

func (t *TCPTransport) Responses() <-chan []byte { return t.responses }
func (t *TCPTransport) Errors() <-chan error     { return t.errors }

func (t *TCPTransport) Shutdown() {
	t.grp.StopAndWait()
}

func (t *TCPTransport) close() {
	err := t.conn.Close()
	if err != nil {
		t.error(err)
	}
}
