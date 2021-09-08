package http3

import (
	"net/http"
	"sync"

	"github.com/lbryio/reflector.go/internal/metrics"

	"github.com/lbryio/lbry.go/v2/extras/stop"
)

type blobRequest struct {
	request  *http.Request
	reply    http.ResponseWriter
	finished *sync.WaitGroup
}

var getReqCh = make(chan *blobRequest, 20000)

func InitWorkers(server *Server, workers int) {
	stopper := stop.New(server.grp)
	for i := 0; i < workers; i++ {
		metrics.RoutinesQueue.WithLabelValues("http3", "worker").Inc()
		go func(worker int) {
			defer metrics.RoutinesQueue.WithLabelValues("http3", "worker").Dec()
			for {
				select {
				case <-stopper.Ch():
				case r := <-getReqCh:
					metrics.HTTP3BlobReqQueue.Dec()
					process(server, r)
				}
			}
		}(i)
	}
}

func enqueue(b *blobRequest) {
	metrics.HTTP3BlobReqQueue.Inc()
	getReqCh <- b
}

func process(server *Server, r *blobRequest) {
	server.HandleGetBlob(r.reply, r.request)
	r.finished.Done()
}
