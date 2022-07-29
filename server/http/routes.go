package http

import (
	"net/http"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/reflector"
	"github.com/lbryio/reflector.go/shared"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

func (s *Server) getBlob(c *gin.Context) {
	waiter := &sync.WaitGroup{}
	waiter.Add(1)
	enqueue(&blobRequest{c: c, finished: waiter})
	waiter.Wait()
}

func (s *Server) HandleGetBlob(c *gin.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered from panic: %v", r)
		}
	}()
	start := time.Now()
	hash := c.Query("hash")
	edgeToken := c.Query("edge_token")

	if reflector.IsProtected(hash) && edgeToken != s.edgeToken {
		_ = c.Error(errors.Err("requested blob is protected"))
		c.String(http.StatusForbidden, "requested blob is protected")
		return
	}
	if s.missesCache.Has(hash) {
		serialized, err := shared.NewBlobTrace(time.Since(start), "http").Serialize()
		c.Header("Via", serialized)
		if err != nil {
			_ = c.Error(errors.Err(err))
			c.String(http.StatusInternalServerError, err.Error())
			return
		}
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	blob, trace, err := s.store.Get(hash)
	if err != nil {
		serialized, serializeErr := trace.Serialize()
		if serializeErr != nil {
			_ = c.Error(errors.Prefix(serializeErr.Error(), err))
			c.String(http.StatusInternalServerError, errors.Prefix(serializeErr.Error(), err).Error())
			return
		}
		c.Header("Via", serialized)

		if errors.Is(err, store.ErrBlobNotFound) {
			_ = s.missesCache.Set(hash, true)
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	serialized, err := trace.Serialize()
	if err != nil {
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	metrics.MtrOutBytesHttp.Add(float64(len(blob)))
	metrics.BlobDownloadCount.Inc()
	metrics.HttpDownloadCount.Inc()
	c.Header("Via", serialized)
	c.Header("Content-Disposition", "filename="+hash)
	c.Data(http.StatusOK, "application/octet-stream", blob)
}

func (s *Server) hasBlob(c *gin.Context) {
	hash := c.Query("hash")
	has, err := s.store.Has(hash)
	if err != nil {
		_ = c.Error(err)
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	if has {
		c.Status(http.StatusNoContent)
		return
	}
	c.Status(http.StatusNotFound)
}

func (s *Server) recoveryHandler(c *gin.Context, err interface{}) {
	c.JSON(500, gin.H{
		"title": "Error",
		"err":   err,
	})
}
