package speedwalk

import (
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/lbryio/reflector.go/internal/metrics"

	"github.com/lbryio/lbry.go/v2/extras/errors"

	"github.com/karrick/godirwalk"
	"github.com/sirupsen/logrus"
)

// AllFiles recursively lists every file in every subdirectory of a given directory
// If basename is true, return the basename of each file. Otherwise return the full path starting at startDir.
func AllFiles(startDir string, basename bool) ([]string, error) {
	entries, err := os.ReadDir(startDir)
	if err != nil {
		return nil, err
	}
	items := make([]fs.FileInfo, 0, len(entries))
	for _, entry := range entries {
		var info fs.FileInfo
		info, err = entry.Info()
		if err != nil {
			return nil, err
		}
		items = append(items, info)
	}

	if err != nil {
		return nil, err
	}

	pathChan := make(chan string)
	paths := make([]string, 0, 1000)
	pathWG := &sync.WaitGroup{}
	pathWG.Add(1)
	metrics.RoutinesQueue.WithLabelValues("speedwalk", "worker").Inc()
	go func() {
		defer pathWG.Done()
		for {
			path, ok := <-pathChan
			if !ok {
				return
			}
			paths = append(paths, path)
		}
	}()

	maxThreads := runtime.NumCPU() - 1
	goroutineLimiter := make(chan struct{}, maxThreads)
	for i := 0; i < maxThreads; i++ {
		goroutineLimiter <- struct{}{}
	}

	walkerWG := &sync.WaitGroup{}
	for _, item := range items {
		if !item.IsDir() {
			if basename {
				pathChan <- item.Name()
			} else {
				pathChan <- filepath.Join(startDir, item.Name())
			}
			continue
		}

		<-goroutineLimiter
		walkerWG.Add(1)

		go func(dir string) {
			defer func() {
				walkerWG.Done()
				goroutineLimiter <- struct{}{}
			}()
			err = godirwalk.Walk(filepath.Join(startDir, dir), &godirwalk.Options{
				Unsorted: true, // faster this way
				Callback: func(osPathname string, de *godirwalk.Dirent) error {
					if de.IsRegular() {
						if basename {
							pathChan <- de.Name()
						} else {
							pathChan <- osPathname
						}
					}
					return nil
				},
			})
			if err != nil {
				logrus.Error(errors.FullTrace(err))
			}
		}(item.Name())
	}

	walkerWG.Wait()

	close(pathChan)
	pathWG.Wait()
	return paths, nil
}
