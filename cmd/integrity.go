package cmd

import (
	"crypto/sha512"
	"encoding/hex"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/reflector.go/meta"
	"github.com/lbryio/reflector.go/store/speedwalk"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var threads int

func init() {
	var cmd = &cobra.Command{
		Use:   "check-integrity",
		Short: "check blobs integrity for a given path",
		Run:   integrityCheckCmd,
	}
	cmd.Flags().StringVar(&diskStorePath, "store-path", "", "path of the store where all blobs are cached")
	cmd.Flags().IntVar(&threads, "threads", runtime.NumCPU()-1, "number of concurrent threads to process blobs")
	rootCmd.AddCommand(cmd)
}

func integrityCheckCmd(cmd *cobra.Command, args []string) {
	log.Printf("reflector %s", meta.VersionString())
	if diskStorePath == "" {
		log.Fatal("store-path must be defined")
	}

	blobs, err := speedwalk.AllFiles(diskStorePath, true)
	if err != nil {
		log.Errorf("error while reading blobs from disk %s", errors.FullTrace(err))
	}
	tasks := make(chan string, len(blobs))
	done := make(chan bool)
	processed := new(int32)
	go produce(tasks, blobs)
	cpus := runtime.NumCPU()
	for i := 0; i < cpus-1; i++ {
		go consume(i, tasks, done, len(blobs), processed)
	}
	<-done
}

func produce(tasks chan<- string, blobs []string) {
	for _, b := range blobs {
		tasks <- b
	}
	close(tasks)
}

func consume(worker int, tasks <-chan string, done chan<- bool, totalTasks int, processed *int32) {
	start := time.Now()

	for b := range tasks {
		checked := atomic.AddInt32(processed, 1)
		if worker == 0 {
			remaining := int32(totalTasks) - checked
			timePerBlob := time.Since(start).Microseconds() / int64(checked)
			remainingTime := time.Duration(int64(remaining)*timePerBlob) * time.Microsecond
			log.Infof("[T%d] %d/%d blobs checked. ETA: %s", worker, checked, totalTasks, remainingTime.String())
		}
		blobPath := path.Join(diskStorePath, b[:2], b)
		blob, err := ioutil.ReadFile(blobPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			log.Errorf("[Worker %d] Error looking up blob %s: %s", worker, b, err.Error())
			continue
		}
		hashBytes := sha512.Sum384(blob)
		readHash := hex.EncodeToString(hashBytes[:])
		if readHash != b {
			log.Infof("[%s] found a broken blob while reading from disk. Actual hash: %s", b, readHash)
			err := os.Remove(blobPath)
			if err != nil {
				log.Errorf("Error while deleting broken blob %s: %s", b, err.Error())
			}
		}
	}
	done <- true
}
