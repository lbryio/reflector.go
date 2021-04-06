package http3

import (
	"net/http"

	"github.com/lbryio/reflector.go/internal/metrics"

	"github.com/lbryio/lbry.go/v2/extras/stop"
)

type blobRequest struct {
	request  *http.Request
	reply    http.ResponseWriter
	finished *stop.Group
}

var getReqCh = make(chan *blobRequest)

func InitWorkers(server *Server, workers int) error {
	stopper := stop.New(server.grp)
	for i := 0; i < workers; i++ {
		go func(worker int) {
			select {
			case <-stopper.Ch():
			case r := <-getReqCh:
				metrics.Http3BlobReqQueue.Dec()
				process(server, r)
			}
		}(i)
	}
	return nil
}

func enqueue(b *blobRequest) {
	metrics.Http3BlobReqQueue.Inc()
	getReqCh <- b
}

func process(server *Server, r *blobRequest) {
	server.HandleGetBlob(r.reply, r.request)
	r.finished.Done()
}
