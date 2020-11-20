package store

import (
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"

	"github.com/lbryio/lbry.go/v2/stream"

	"golang.org/x/sync/singleflight"
)

func WithSingleFlight(component string, origin BlobStore) BlobStore {
	return &singleflightStore{
		BlobStore: origin,
		component: component,
		sf:        new(singleflight.Group),
	}
}

type singleflightStore struct {
	BlobStore

	component string
	sf        *singleflight.Group
}

func (s *singleflightStore) Name() string {
	return "sf_" + s.BlobStore.Name()
}

// Get ensures that only one request per hash is sent to the origin at a time,
// thereby protecting against https://en.wikipedia.org/wiki/Thundering_herd_problem
func (s *singleflightStore) Get(hash string) (stream.Blob, error) {
	metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
	defer metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

	blob, err, _ := s.sf.Do(hash, s.getter(hash))
	if err != nil {
		return nil, err
	}
	return blob.(stream.Blob), nil
}

// getter returns a function that gets a blob from the origin
// only one getter per hash will be executing at a time
func (s *singleflightStore) getter(hash string) func() (interface{}, error) {
	return func() (interface{}, error) {
		metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
		defer metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

		start := time.Now()
		blob, err := s.BlobStore.Get(hash)
		if err != nil {
			return nil, err
		}

		rate := float64(len(blob)) / 1024 / 1024 / time.Since(start).Seconds()
		metrics.CacheRetrievalSpeed.With(map[string]string{
			metrics.LabelCacheType: s.Name(),
			metrics.LabelComponent: s.component,
			metrics.LabelSource:    "origin",
		}).Set(rate)

		return blob, nil
	}
}
