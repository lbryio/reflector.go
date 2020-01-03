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

const (
	ns = "reflector"

	labelDirection = "direction"
	labelErrorType = "error_type"

	DirectionUpload   = "upload"   // to reflector
	DirectionDownload = "download" // from reflector

	errConnReset        = "conn_reset"
	errReadConnReset    = "read_conn_reset"
	errWriteConnReset   = "write_conn_reset"
	errReadConnTimedOut = "read_conn_timed_out"
	errWriteBrokenPipe  = "write_broken_pipe"
	errEPipe            = "e_pipe"
	errETimedout        = "e_timedout"
	errIOTimeout        = "io_timeout"
	errUnexpectedEOF    = "unexpected_eof"
	errUnexpectedEOFStr = "unexpected_eof_str"
	errJSONSyntax       = "json_syntax"
	errBlobTooBig       = "blob_too_big"
	errDeadlineExceeded = "deadline_exceeded"
	errOther            = "other"
)

var (
	BlobDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "blob_download_total",
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
	ErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_total",
		Help:      "Total number of errors",
	}, []string{labelDirection, labelErrorType})
)

func TrackError(direction string, e error) (shouldLog bool) { // shouldLog is a hack, but whatever
	if e == nil {
		return
	}

	err := ee.Wrap(e, 0)
	errType := errOther
	if strings.Contains(err.Error(), "i/o timeout") { // hit a read or write deadline
		errType = errIOTimeout
	} else if errors.Is(e, syscall.ECONNRESET) {
		// Looks like we're getting this when direction == "download", but read_conn_reset when its "upload"
		errType = errConnReset
	} else if errors.Is(e, context.DeadlineExceeded) {
		errType = errDeadlineExceeded
	} else if strings.Contains(err.Error(), "read: connection reset by peer") { // the other side closed the connection using TCP reset
		errType = errReadConnReset
	} else if strings.Contains(err.Error(), "write: connection reset by peer") { // the other side closed the connection using TCP reset
		log.Warnln("write conn reset by peer is not the same as ECONNRESET")
		errType = errWriteConnReset
	} else if errors.Is(e, syscall.ETIMEDOUT) {
		errType = errETimedout
	} else if strings.Contains(err.Error(), "read: connection timed out") { // the other side closed the connection using TCP reset
		//log.Warnln("read conn timed out is not the same as ETIMEDOUT")
		errType = errReadConnTimedOut
	} else if errors.Is(e, io.ErrUnexpectedEOF) {
		errType = errUnexpectedEOF
	} else if strings.Contains(err.Error(), "unexpected EOF") { // tried to read from closed pipe or socket
		errType = errUnexpectedEOFStr
	} else if errors.Is(e, syscall.EPIPE) {
		errType = errEPipe
	} else if strings.Contains(err.Error(), "write: broken pipe") { // tried to write to a pipe or socket that was closed by the peer
		// I believe this is the same as EPipe when direction == "download", but not for upload
		errType = errWriteBrokenPipe
		//} else if errors.Is(e, reflector.ErrBlobTooBig) { # this creates a circular import
		//	errType = errBlobTooBig
	} else if strings.Contains(err.Error(), "blob must be at most") {
		//log.Warnln("blob must be at most X bytes is not the same as ErrBlobTooBig")
		errType = errBlobTooBig
	} else if _, ok := e.(*json.SyntaxError); ok {
		errType = errJSONSyntax
	} else {
		log.Warnf("error '%s' for direction '%s' is not being tracked", err.TypeName(), direction)
		shouldLog = true
	}

	ErrorCount.With(map[string]string{
		labelDirection: direction,
		labelErrorType: errType,
	}).Inc()

	return
}
