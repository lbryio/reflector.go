package reflector

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/stop"

	log "github.com/sirupsen/logrus"
)

// TODO: store daily stats too. and maybe other intervals

type stats struct {
	mu      *sync.Mutex
	blobs   int
	streams int
	errors  map[string]int

	logger  *log.Logger
	logFreq time.Duration
	grp     *stop.Group
}

func newStatLogger(logger *log.Logger, logFreq time.Duration, parentGrp *stop.Group) *stats {
	return &stats{
		mu:      &sync.Mutex{},
		grp:     stop.New(parentGrp),
		logger:  logger,
		logFreq: logFreq,
		errors:  make(map[string]int),
	}
}

func (s *stats) Start() {
	s.grp.Add(1)
	go func() {
		defer s.grp.Done()
		s.runSlackLogger()
	}()
}

func (s *stats) Shutdown() {
	s.log()
	s.grp.StopAndWait()
}

func (s *stats) AddBlob() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blobs++
}
func (s *stats) AddStream() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.streams++
}

func (s *stats) AddError(e error) (shouldLog bool) { // shouldLog is a hack, but whatever
	if e == nil {
		return
	}
	err := errors.Wrap(e, 0)
	name := err.TypeName()
	if strings.Contains(err.Error(), "i/o timeout") {
		name = "i/o timeout"
	} else if strings.Contains(err.Error(), "read: connection reset by peer") {
		name = "read conn reset"
	} else {
		shouldLog = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors[name]++
	return
}

func (s *stats) runSlackLogger() {
	t := time.NewTicker(s.logFreq)
	for {
		select {
		case <-s.grp.Ch():
			return
		case <-t.C:
			s.log()
		}
	}
}

func (s *stats) log() {
	s.mu.Lock()
	blobs, streams := s.blobs, s.streams
	s.blobs, s.streams = 0, 0
	errStr := ""
	for name, count := range s.errors {
		errStr += fmt.Sprintf("%d %s, ", count, name)
		delete(s.errors, name)
	}
	s.mu.Unlock()

	if len(errStr) > 2 {
		errStr = errStr[:len(errStr)-2] // trim last comma and space
	}

	s.logger.Printf("Stats: %d blobs, %d streams, errors: %s", blobs, streams, errStr)
}
