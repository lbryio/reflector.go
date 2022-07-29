package http3

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/gorilla/mux"
	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
	log "github.com/sirupsen/logrus"
)

// Server is an instance of a peer server that houses the listener and store.
type Server struct {
	store              store.BlobStore
	grp                *stop.Group
	concurrentRequests int
}

// NewServer returns an initialized Server pointer.
func NewServer(store store.BlobStore, requestQueueSize int) *Server {
	return &Server{
		store:              store,
		grp:                stop.New(),
		concurrentRequests: requestQueueSize,
	}
}

// Shutdown gracefully shuts down the peer server.
func (s *Server) Shutdown() {
	log.Debug("shutting down http3 peer server")
	s.grp.StopAndWait()
	log.Debug("http3 peer server stopped")
}

func (s *Server) logError(e error) {
	if e == nil {
		return
	}
	shouldLog := metrics.TrackError(metrics.DirectionDownload, e)
	if shouldLog {
		log.Errorln(errors.FullTrace(e))
	}
}

type availabilityResponse struct {
	LbrycrdAddress string `json:"lbrycrd_address"`
	IsAvailable    bool   `json:"is_available"`
}

// Start starts the server listener to handle connections.
func (s *Server) Start(address string) error {
	log.Println("HTTP3 peer listening on " + address)
	window500M := 500 * 1 << 20

	quicConf := &quic.Config{
		MaxStreamReceiveWindow:     uint64(window500M),
		MaxConnectionReceiveWindow: uint64(window500M),
		EnableDatagrams:            true,
		HandshakeIdleTimeout:       4 * time.Second,
		MaxIdleTimeout:             20 * time.Second,
	}
	r := mux.NewRouter()
	r.HandleFunc("/get/{hash}", func(w http.ResponseWriter, r *http.Request) {
		waiter := &sync.WaitGroup{}
		waiter.Add(1)
		enqueue(&blobRequest{request: r, reply: w, finished: waiter})
		waiter.Wait()
	})
	r.HandleFunc("/has/{hash}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		requestedBlob := vars["hash"]
		blobExists, err := s.store.Has(requestedBlob)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			s.logError(err)
			return
		}
		if !blobExists {
			w.WriteHeader(http.StatusNotFound)
		}
		// LbrycrdAddress to be used when paying for data. Not implemented yet.
		const LbrycrdAddress = "bJxKvpD96kaJLriqVajZ7SaQTsWWyrGQct"
		resp, err := json.Marshal(availabilityResponse{
			LbrycrdAddress: LbrycrdAddress,
			IsAvailable:    blobExists,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			s.logError(err)
			return
		}
		_, err = w.Write(resp)
		if err != nil {
			s.logError(err)
		}
	})
	server := http3.Server{
		Server: &http.Server{
			Handler:   r,
			Addr:      address,
			TLSConfig: generateTLSConfig(),
		},
		QuicConfig: quicConf,
	}
	go InitWorkers(s, s.concurrentRequests)
	go s.listenForShutdown(&server)
	s.grp.Add(1)
	go func() {
		s.listenAndServe(&server)
		s.grp.Done()
	}()

	return nil
}

// Setup a bare-bones TLS config for the server
func generateTLSConfig() *tls.Config {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{SerialNumber: big.NewInt(1)}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"http3-reflector-server"},
	}
}

func (s *Server) listenAndServe(server *http3.Server) {
	err := server.ListenAndServe()
	if err != nil && err.Error() != "server closed" {
		log.Errorln(errors.FullTrace(err))
	}
}

func (s *Server) listenForShutdown(listener *http3.Server) {
	<-s.grp.Ch()
	err := listener.Close()
	if err != nil {
		log.Error("error closing listener for peer server - ", err)
	}
}

func (s *Server) HandleGetBlob(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	requestedBlob := vars["hash"]
	traceParam := r.URL.Query().Get("trace")
	var err error
	wantsTrace := false
	if traceParam != "" {
		wantsTrace, err = strconv.ParseBool(traceParam)
		if err != nil {
			wantsTrace = false
		}
	}
	if reflector.IsProtected(requestedBlob) {
		http.Error(w, "requested blob is protected", http.StatusForbidden)
		return
	}
	blob, trace, err := s.store.Get(requestedBlob)

	if wantsTrace {
		serialized, err := trace.Serialize()
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Add("Via", serialized)
		log.Debug(trace.String())
	}
	if err != nil {
		if errors.Is(err, store.ErrBlobNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		fmt.Printf("%s: %s", requestedBlob, errors.FullTrace(err))
		s.logError(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = w.Write(blob)
	if err != nil {
		s.logError(err)
	}
	metrics.MtrOutBytesUdp.Add(float64(len(blob)))
	metrics.BlobDownloadCount.Inc()
	metrics.Http3DownloadCount.Inc()
}
