package store

import (
	"strings"
	"time"

	"github.com/lbryio/reflector.go/internal/metrics"
	"github.com/lbryio/reflector.go/shared"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/spf13/viper"
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

type SingleFlightConfig struct {
	Component string `mapstructure:"component"`
	Store     *viper.Viper
}

func SingleFlightStoreFactory(config *viper.Viper) (BlobStore, error) {
	var cfg SingleFlightConfig
	err := config.Unmarshal(&cfg)
	if err != nil {
		return nil, errors.Err(err)
	}

	cfg.Store = config.Sub("store")

	storeType := strings.Split(cfg.Store.AllKeys()[0], ".")[0]
	storeConfig := cfg.Store.Sub(storeType)
	factory, ok := Factories[storeType]
	if !ok {
		return nil, errors.Err("unknown store type %s", storeType)
	}
	underlyingStore, err := factory(storeConfig)
	if err != nil {
		return nil, errors.Err(err)
	}

	return WithSingleFlight(cfg.Component, underlyingStore), nil
}

func init() {
	RegisterStore("singleflight", SingleFlightStoreFactory)
}

func (s *singleflightStore) Name() string {
	return "sf_" + s.BlobStore.Name()
}

type getterResponse struct {
	blob  stream.Blob
	stack shared.BlobTrace
}

// Get ensures that only one request per hash is sent to the origin at a time,
// thereby protecting against https://en.wikipedia.org/wiki/Thundering_herd_problem
func (s *singleflightStore) Get(hash string) (stream.Blob, shared.BlobTrace, error) {
	start := time.Now()
	metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
	defer metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

	gr, err, _ := s.sf.Do(hash, s.getter(hash))
	if err != nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), err
	}
	if gr == nil {
		return nil, shared.NewBlobTrace(time.Since(start), s.Name()), errors.Err("getter response is nil")
	}
	rsp := gr.(getterResponse)
	return rsp.blob, rsp.stack, nil
}

// getter returns a function that gets a blob from the origin
// only one getter per hash will be executing at a time
func (s *singleflightStore) getter(hash string) func() (interface{}, error) {
	return func() (interface{}, error) {
		metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
		defer metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

		start := time.Now()
		blob, stack, err := s.BlobStore.Get(hash)
		if err != nil {
			return getterResponse{
				blob:  nil,
				stack: stack.Stack(time.Since(start), s.Name()),
			}, err
		}

		rate := float64(len(blob)) / 1024 / 1024 / time.Since(start).Seconds()
		metrics.CacheRetrievalSpeed.With(map[string]string{
			metrics.LabelCacheType: s.Name(),
			metrics.LabelComponent: s.component,
			metrics.LabelSource:    "origin",
		}).Set(rate)

		return getterResponse{
			blob:  blob,
			stack: stack.Stack(time.Since(start), s.Name()),
		}, nil
	}
}

// Put ensures that only one request per hash is sent to the origin at a time,
// thereby protecting against https://en.wikipedia.org/wiki/Thundering_herd_problem
func (s *singleflightStore) Put(hash string, blob stream.Blob) error {
	metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
	defer metrics.CacheWaitingRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

	_, err, _ := s.sf.Do(hash, s.putter(hash, blob))
	if err != nil {
		return err
	}
	return nil
}

// putter returns a function that puts a blob from the origin
// only one putter per hash will be executing at a time
func (s *singleflightStore) putter(hash string, blob stream.Blob) func() (interface{}, error) {
	return func() (interface{}, error) {
		metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Inc()
		defer metrics.CacheOriginRequestsCount.With(metrics.CacheLabels(s.Name(), s.component)).Dec()

		start := time.Now()
		err := s.BlobStore.Put(hash, blob)
		if err != nil {
			return nil, err
		}

		rate := float64(len(blob)) / 1024 / 1024 / time.Since(start).Seconds()
		metrics.CacheRetrievalSpeed.With(map[string]string{
			metrics.LabelCacheType: s.Name(),
			metrics.LabelComponent: s.component,
			metrics.LabelSource:    "origin",
		}).Set(rate)

		return nil, nil
	}
}

// Shutdown shuts down the store gracefully
func (s *singleflightStore) Shutdown() {
	s.BlobStore.Shutdown()
}
