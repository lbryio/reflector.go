package http

import (
	"context"
	"net/http"
	"time"

	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/bluele/gcache"
	nice "github.com/ekyoung/gin-nice-recovery"
	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// Server is an instance of a peer server that houses the listener and store.
type Server struct {
	store              store.BlobStore
	grp                *stop.Group
	concurrentRequests int
	missesCache        gcache.Cache
	edgeToken          string
	address            string
}

// NewServer returns an initialized Server pointer.
func NewServer(store store.BlobStore, requestQueueSize int, edgeToken string, address string) *Server {
	return &Server{
		store:              store,
		grp:                stop.New(),
		concurrentRequests: requestQueueSize,
		missesCache:        gcache.New(2000).Expiration(5 * time.Minute).ARC().Build(),
		edgeToken:          edgeToken,
		address:            address,
	}
}

// Shutdown gracefully shuts down the peer server.
func (s *Server) Shutdown() {
	log.Debug("shutting down HTTP server")
	s.grp.StopAndWait()
	log.Debug("HTTP server stopped")
}

// Start starts the server listener to handle connections.
func (s *Server) Start() error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Logger())
	// Install nice.Recovery, passing the handler to call after recovery
	router.Use(nice.Recovery(s.recoveryHandler))
	router.GET("/blob", s.getBlob)
	router.HEAD("/blob", s.hasBlob)
	srv := &http.Server{
		Addr:    s.address,
		Handler: router,
	}
	go s.listenForShutdown(srv)
	go InitWorkers(s, s.concurrentRequests)
	// Initializing the server in a goroutine so that
	// it won't block the graceful shutdown handling below
	s.grp.Add(1)
	go func() {
		defer s.grp.Done()
		log.Println("HTTP server listening on " + s.address)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()
	return nil
}

func (s *Server) listenForShutdown(listener *http.Server) {
	<-s.grp.Ch()
	// The context is used to inform the server it has 5 seconds to finish
	// the request it is currently handling
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := listener.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}
