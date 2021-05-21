package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
)

func (s *Server) getBlob(c *gin.Context) {
	hash := c.Query("hash")
	blob, trace, err := s.store.Get(hash)
	if err != nil {
		serialized, serializeErr := trace.Serialize()
		if serializeErr != nil {
			_ = c.AbortWithError(http.StatusInternalServerError, errors.Prefix(serializeErr.Error(), err))
			return
		}
		c.Header("Via", serialized)

		if errors.Is(err, store.ErrBlobNotFound) {
			log.Errorf("wtf: %s", err.Error())
			c.AbortWithStatus(http.StatusNotFound)
			return
		}
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	serialized, err := trace.Serialize()
	if err != nil {
		_ = c.AbortWithError(http.StatusInternalServerError, err)
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
		_ = c.AbortWithError(http.StatusInternalServerError, err)
		return
	}
	if has {
		c.Status(http.StatusNoContent)
		return
	}
	c.Status(http.StatusNotFound)
}
