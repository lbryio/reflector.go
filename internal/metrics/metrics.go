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
	_ = s.srv.Shutdown(context.Background())
	s.stop.StopAndWait()
}

const (
	ns             = "reflector"
	subsystemCache = "cache"
	subsystemITTT  = "ittt"

	labelDirection = "direction"
	labelErrorType = "error_type"

	DirectionUpload   = "upload"   // to reflector
	DirectionDownload = "download" // from reflector

	LabelCacheType = "cache_type"
	LabelComponent = "component"
	LabelSource    = "source"

	errConnReset         = "conn_reset"
	errReadConnReset     = "read_conn_reset"
	errWriteConnReset    = "write_conn_reset"
	errReadConnTimedOut  = "read_conn_timed_out"
	errNoNetworkActivity = "no_network_activity"
	errWriteConnTimedOut = "write_conn_timed_out"
	errWriteBrokenPipe   = "write_broken_pipe"
	errEPipe             = "e_pipe"
	errETimedout         = "e_timedout"
	errIOTimeout         = "io_timeout"
	errUnexpectedEOF     = "unexpected_eof"
	errUnexpectedEOFStr  = "unexpected_eof_str"
	errJSONSyntax        = "json_syntax"
	errBlobTooBig        = "blob_too_big"
	errInvalidPeerJSON   = "invalid_peer_json"
	errInvalidPeerData   = "invalid_peer_data"
	errRequestTooLarge   = "request_too_large"
	errDeadlineExceeded  = "deadline_exceeded"
	errHashMismatch      = "hash_mismatch"
	errProtectedBlob     = "protected_blob"
	errInvalidBlobHash   = "invalid_blob_hash"
	errZeroByteBlob      = "zero_byte_blob"
	errInvalidCharacter  = "invalid_character"
	errBlobNotFound      = "blob_not_found"
	errNoErr             = "no_error"
	errQuicProto         = "quic_protocol_violation"
	errOther             = "other"
)

var (
	ErrorCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "error_total",
		Help:      "Total number of errors",
	}, []string{labelDirection, labelErrorType})

	BlobDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "blob_download_total",
		Help:      "Total number of blobs downloaded from reflector",
	})
	PeerDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "peer_download_total",
		Help:      "Total number of blobs downloaded from reflector through tcp protocol",
	})
	Http3DownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "http3_blob_download_total",
		Help:      "Total number of blobs downloaded from reflector through QUIC protocol",
	})
	HttpDownloadCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "http_blob_download_total",
		Help:      "Total number of blobs downloaded from reflector through HTTP protocol",
	})

	CacheHitCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Subsystem: subsystemCache,
		Name:      "hit_total",
		Help:      "Total number of blobs retrieved from the cache storage",
	}, []string{LabelCacheType, LabelComponent})
	ThisHitCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Subsystem: subsystemITTT,
		Name:      "this_hit_total",
		Help:      "Total number of blobs retrieved from the this storage",
	})
	ThatHitCount = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Subsystem: subsystemITTT,
		Name:      "that_hit_total",
		Help:      "Total number of blobs retrieved from the that storage",
	})
	CacheMissCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Subsystem: subsystemCache,
		Name:      "miss_total",
		Help:      "Total number of blobs retrieved from origin rather than cache storage",
	}, []string{LabelCacheType, LabelComponent})
	CacheOriginRequestsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Subsystem: subsystemCache,
		Name:      "origin_requests_total",
		Help:      "How many Get requests are in flight from the cache to the origin",
	}, []string{LabelCacheType, LabelComponent})
	//during thundering-herd situations, the metric below should be a lot smaller than the metric above
	CacheWaitingRequestsCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Subsystem: subsystemCache,
		Name:      "waiting_requests_total",
		Help:      "How many cache requests are waiting for an in-flight origin request",
	}, []string{LabelCacheType, LabelComponent})
	CacheLRUEvictCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: ns,
		Subsystem: subsystemCache,
		Name:      "evict_total",
		Help:      "Count of blobs evicted from cache",
	}, []string{LabelCacheType, LabelComponent})
	CacheRetrievalSpeed = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "speed_mbps",
		Help:      "Speed of blob retrieval from cache or from origin",
	}, []string{LabelCacheType, LabelComponent, LabelSource})

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

	MtrInBytesTcp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "tcp_in_bytes",
		Help:      "Total number of bytes downloaded through TCP",
	})
	MtrOutBytesTcp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "tcp_out_bytes",
		Help:      "Total number of bytes streamed out through TCP",
	})
	MtrInBytesUdp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "udp_in_bytes",
		Help:      "Total number of bytes downloaded through UDP",
	})
	MtrInBytesHttp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "http_in_bytes",
		Help:      "Total number of bytes downloaded through HTTP",
	})
	MtrOutBytesUdp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "udp_out_bytes",
		Help:      "Total number of bytes streamed out through UDP",
	})
	MtrOutBytesHttp = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "http_out_bytes",
		Help:      "Total number of bytes streamed out through UDP",
	})
	MtrInBytesReflector = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "reflector_in_bytes",
		Help:      "Total number of incoming bytes (from users)",
	})
	MtrOutBytesReflector = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "s3_out_bytes",
		Help:      "Total number of outgoing bytes (to S3)",
	})
	MtrInBytesUpstream = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "upstream_in_bytes",
		Help:      "Total number of incoming bytes (from Upstream)",
	})
	MtrInBytesS3 = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: ns,
		Name:      "s3_in_bytes",
		Help:      "Total number of incoming bytes (from S3-CF)",
	})
	Http3BlobReqQueue = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "http3_blob_request_queue_size",
		Help:      "Blob requests of https queue size",
	})
	HttpBlobReqQueue = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "http_blob_request_queue_size",
		Help:      "Blob requests queue size of the HTTP protocol",
	})
	RoutinesQueue = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns,
		Name:      "routines",
		Help:      "routines running by type",
	}, []string{"package", "kind"})
)

func CacheLabels(name, component string) prometheus.Labels {
	return prometheus.Labels{
		LabelCacheType: name,
		LabelComponent: component,
	}
}

func TrackError(direction string, e error) (shouldLog bool) { // shouldLog is a hack, but whatever
	if e == nil {
		return
	}

	err := ee.Wrap(e, 0)
	errType := errOther
	if strings.Contains(err.Error(), "i/o timeout") {
		errType = errIOTimeout
	} else if errors.Is(e, syscall.ECONNRESET) {
		// Looks like we're getting this when direction == "download", but read_conn_reset and
		// write_conn_reset when its "upload"
		errType = errConnReset
	} else if errors.Is(e, context.DeadlineExceeded) {
		errType = errDeadlineExceeded
	} else if strings.Contains(err.Error(), "read: connection reset by peer") { // the other side closed the connection using TCP reset
		errType = errReadConnReset
	} else if strings.Contains(err.Error(), "write: connection reset by peer") { // the other side closed the connection using TCP reset
		errType = errWriteConnReset
	} else if errors.Is(e, syscall.ETIMEDOUT) {
		errType = errETimedout
	} else if strings.Contains(err.Error(), "read: connection timed out") { // the other side closed the connection using TCP reset
		//log.Warnln("read conn timed out is not the same as ETIMEDOUT")
		errType = errReadConnTimedOut
	} else if strings.Contains(err.Error(), "NO_ERROR: No recent network activity") { // the other side closed the QUIC connection
		//log.Warnln("read conn timed out is not the same as ETIMEDOUT")
		errType = errNoNetworkActivity
	} else if strings.Contains(err.Error(), "write: connection timed out") {
		errType = errWriteConnTimedOut
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
	} else if strings.Contains(err.Error(), "invalid json request") {
		errType = errInvalidPeerJSON
	} else if strings.Contains(err.Error(), "Invalid data") {
		errType = errInvalidPeerData
	} else if strings.Contains(err.Error(), "request is too large") {
		errType = errRequestTooLarge
	} else if strings.Contains(err.Error(), "Invalid blob hash length") {
		errType = errInvalidBlobHash
	} else if strings.Contains(err.Error(), "hash of received blob data does not match hash from send request") {
		errType = errHashMismatch
	} else if strings.Contains(err.Error(), "blob not found") {
		errType = errBlobNotFound
	} else if strings.Contains(err.Error(), "requested blob is protected") {
		errType = errProtectedBlob
	} else if strings.Contains(err.Error(), "0-byte blob received") {
		errType = errZeroByteBlob
	} else if strings.Contains(err.Error(), "PROTOCOL_VIOLATION: tried to retire connection") {
		errType = errQuicProto
	} else if strings.Contains(err.Error(), "invalid character") {
		errType = errInvalidCharacter
	} else if _, ok := e.(*json.SyntaxError); ok {
		errType = errJSONSyntax
	} else if strings.Contains(err.Error(), "NO_ERROR") {
		errType = errNoErr
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
