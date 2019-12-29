package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"syscall"
	"time"

	ee "github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type Server struct {
	srv  *http.Server
	stop *stop.Stopper
}

func NewServer(address string, path string) *Server {
	h := http.NewServeMux()
	h.Handle(path, promhttp.Handler())
	return &Server{
		srv: &http.Server{
			Addr:    address,
			Handler: h,
			//https://blog.cloudflare.com/the-complete-guide-to-golang-net-http-timeouts/
			//https://blog.cloudflare.com/exposing-go-on-the-internet/
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  120 * time.Second,
		},
		stop: stop.New(),
	}
}

func (s *Server) Start() {
	s.stop.Add(1)
	go func() {
		defer s.stop.Done()
		err := s.srv.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error(err)
		}
	}()
}

func (s *Server) Shutdown() {
	s.srv.Shutdown(context.Background())
	s.stop.StopAndWait()
}

const ns = "reflector"

var (
	BlobDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "download_total",
		Help:      "Total number of blobs downloaded from reflector",
	})
	BlobUploadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "blob_upload_total",
		Help:      "Total number of blobs uploaded to reflector",
	})
	SDBlobUploadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "sdblob_upload_total",
		Help:      "Total number of SD blobs (and therefore streams) uploaded to reflector",
	})
	ErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_total",
		Help:      "Total number of errors",
	})
	IOTimeoutCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_io_timeout_total",
		Help:      "Total number of 'i/o timeout' errors",
	})
	ReadConnResetCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_read_conn_reset_total",
		Help:      "Total number of 'read: connection reset by peer' errors",
	})
	UnexpectedEOFCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_unexpected_eof_total",
		Help:      "Total number of 'unexpected EOF' errors",
	})
	BrokenPipeCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_broken_pipe_total",
		Help:      "Total number of 'write: broken pipe' errors",
	})
	JSONSyntaxErrorCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_json_syntax_total",
		Help:      "Total number of JSON syntax errors",
	})
)

func TrackError(e error) (shouldLog bool) { // shouldLog is a hack, but whatever
	if e == nil {
		return
	}

	ErrorCount.Inc()

	err := ee.Wrap(e, 0)
	//name := err.TypeName()
	if errors.Is(e, context.DeadlineExceeded) {
		IOTimeoutCount.Inc()
	} else if strings.Contains(err.Error(), "i/o timeout") { // hit a read or write deadline
		log.Warnln("i/o timeout is not the same as context.DeadlineExceeded")
		IOTimeoutCount.Inc()
	} else if errors.Is(e, syscall.ECONNRESET) {
		ReadConnResetCount.Inc()
	} else if strings.Contains(err.Error(), "read: connection reset by peer") { // the other side closed the connection using TCP reset
		log.Warnln("conn reset by peer is not the same as ECONNRESET")
		ReadConnResetCount.Inc()
	} else if errors.Is(e, io.ErrUnexpectedEOF) {
		UnexpectedEOFCount.Inc()
	} else if strings.Contains(err.Error(), "unexpected EOF") { // tried to read from closed pipe or socket
		log.Warnln("unexpected eof is not the same as io.ErrUnexpectedEOF")
		UnexpectedEOFCount.Inc()
	} else if errors.Is(e, syscall.EPIPE) {
		BrokenPipeCount.Inc()
	} else if strings.Contains(err.Error(), "write: broken pipe") { // tried to write to a pipe or socket that was closed by the peer
		log.Warnln("broken pipe is not the same as EPIPE")
		BrokenPipeCount.Inc()
	} else {
		shouldLog = true
	}

	if _, ok := e.(*json.SyntaxError); ok {
		JSONSyntaxErrorCount.Inc()
	}

	return
}
