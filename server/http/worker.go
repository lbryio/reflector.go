package http

import (
	"sync"

	"github.com/lbryio/reflector.go/internal/metrics"

	"github.com/lbryio/lbry.go/v2/extras/stop"

	"github.com/gin-gonic/gin"
)

type blobRequest struct {
	c        *gin.Context
	finished *sync.WaitGroup
}

var getReqCh = make(chan *blobRequest, 20000)

func InitWorkers(server *Server, workers int) {
	stopper := stop.New(server.grp)
	for i := 0; i < workers; i++ {
		metrics.RoutinesQueue.WithLabelValues("http", "worker").Inc()
		go func(worker int) {
			defer metrics.RoutinesQueue.WithLabelValues("http", "worker").Dec()
			for {
				select {
				case <-stopper.Ch():
				case r := <-getReqCh:
					process(server, r)
					metrics.HTTPBlobReqQueue.Dec()
				}
			}
		}(i)
	}
}

func enqueue(b *blobRequest) {
	metrics.HTTPBlobReqQueue.Inc()
	getReqCh <- b
}

func process(server *Server, r *blobRequest) {
	server.HandleGetBlob(r.c)
	r.finished.Done()
}
