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
		go func(worker int) {
			for {
				select {
				case <-stopper.Ch():
				case r := <-getReqCh:
					metrics.Http3BlobReqQueue.Dec()
					process(server, r)
				}
			}
		}(i)
	}
	return
}

func enqueue(b *blobRequest) {
	metrics.Http3BlobReqQueue.Inc()
	getReqCh <- b
}

func process(server *Server, r *blobRequest) {
	server.HandleGetBlob(r.reply, r.request)
	r.finished.Done()
}
