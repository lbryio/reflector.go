package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lbryio/lbry.go/stopOnce"
	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var workers int

const (
	sdInc   = 1
	blobInc = 2
	errInc  = 3
)

type uploaderParams struct {
	workerWG     *sync.WaitGroup
	counterWG    *sync.WaitGroup
	stopper      *stopOnce.Stopper
	filenameChan chan string
	countChan    chan int
	sdCount      int
	blobCount    int
	errCount     int
}

func init() {
	var cmd = &cobra.Command{
		Use:   "upload DIR",
		Short: "Upload blobs to S3",
		Args:  cobra.ExactArgs(1),
		Run:   uploadCmd,
	}
	cmd.PersistentFlags().IntVar(&workers, "workers", 1, "How many worker threads to run at once")
	RootCmd.AddCommand(cmd)
}

func uploadCmd(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	db := new(db.SQL)
	err := db.Connect(GlobalConfig.DBConn)
	checkErr(err)

	params := uploaderParams{
		workerWG:     &sync.WaitGroup{},
		counterWG:    &sync.WaitGroup{},
		filenameChan: make(chan string),
		countChan:    make(chan int),
		stopper:      stopOnce.New()}

	setInterrupt(&params)

	filenames, err := getFileNames(args[0])
	checkErr(err)

	totalCount := len(filenames)

	log.Println("checking for existing blobs")

	exists, err := db.HasBlobs(filenames)
	checkErr(err)
	existsCount := len(exists)

	log.Printf("%d new blobs to upload", totalCount-existsCount)

	startUploadWorkers(&params, args[0])
	params.counterWG.Add(1)
	go func() {
		defer params.counterWG.Done()
		runCountReceiver(&params, startTime, totalCount, existsCount)
	}()

Upload:
	for _, filename := range filenames {
		if exists[filename] {
			continue
		}

		select {
		case params.filenameChan <- filename:
		case <-params.stopper.Ch():
			log.Warnln("Caught interrupt, quitting at first opportunity...")
			break Upload
		}
	}

	close(params.filenameChan)
	params.workerWG.Wait()
	close(params.countChan)
	params.counterWG.Wait()
	params.stopper.Stop()

	log.Println("SUMMARY")
	log.Printf("%d blobs total", totalCount)
	log.Printf("%d SD blobs uploaded", params.sdCount)
	log.Printf("%d content blobs uploaded", params.blobCount)
	log.Printf("%d blobs already stored", existsCount)
	log.Printf("%d errors encountered", params.errCount)
}

func isJSON(data []byte) bool {
	var js json.RawMessage
	return json.Unmarshal(data, &js) == nil
}

func newBlobStore() *store.DBBackedS3Store {
	db := new(db.SQL)
	err := db.Connect(GlobalConfig.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(GlobalConfig.AwsID, GlobalConfig.AwsSecret, GlobalConfig.BucketRegion, GlobalConfig.BucketName)
	return store.NewDBBackedS3Store(s3, db)
}

func setInterrupt(params *uploaderParams) {
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interruptChan
		params.stopper.Stop()
	}()
}

func startUploadWorkers(params *uploaderParams, dir string) {
	for i := 0; i < workers; i++ {
		params.workerWG.Add(1)
		go func(i int) {
			defer params.workerWG.Done()
			defer func(i int) {
				log.Printf("worker %d quitting", i)
			}(i)

			blobStore := newBlobStore()
			launchFileUploader(params, blobStore, dir, i)
		}(i)
	}
}

func launchFileUploader(params *uploaderParams, blobStore *store.DBBackedS3Store, dir string, worker int) {
	for {
		select {
		case <-params.stopper.Ch():
			return
		case filename, ok := <-params.filenameChan:
			if !ok {
				return
			}

			blob, err := ioutil.ReadFile(dir + "/" + filename)
			checkErr(err)

			hash := peer.GetBlobHash(blob)
			if hash != filename {
				log.Errorf("worker %d: filename does not match hash (%s != %s), skipping", worker, filename, hash)
				select {
				case params.countChan <- errInc:
				case <-params.stopper.Ch():
				}
				continue
			}

			if isJSON(blob) {
				log.Printf("worker %d: PUTTING SD BLOB %s", worker, hash)
				if err := blobStore.PutSD(hash, blob); err != nil {
					log.Error("PutSD Error: ", err)
				}
				select {
				case params.countChan <- sdInc:
				case <-params.stopper.Ch():
				}
			} else {
				log.Printf("worker %d: putting %s", worker, hash)
				if err := blobStore.Put(hash, blob); err != nil {
					log.Error("Put Blob Error: ", err)
				}
				select {
				case params.countChan <- blobInc:
				case <-params.stopper.Ch():
				}
			}
		}
	}
}

func runCountReceiver(params *uploaderParams, startTime time.Time, totalCount int, existsCount int) {
	for {
		select {
		case <-params.stopper.Ch():
			return
		case countType, ok := <-params.countChan:
			if !ok {
				return
			}
			switch countType {
			case sdInc:
				params.sdCount++
			case blobInc:
				params.blobCount++
			case errInc:
				params.errCount++
			}
		}
		if (params.sdCount+params.blobCount)%50 == 0 {
			log.Printf("%d of %d done (%s elapsed, %.3fs per blob)", params.sdCount+params.blobCount, totalCount-existsCount, time.Since(startTime).String(), time.Since(startTime).Seconds()/float64(params.sdCount+params.blobCount))
		}
	}
}

func getFileNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	files, err := f.Readdir(-1)
	if err != nil {
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}

	var filenames []string
	for _, file := range files {
		if !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}

	return filenames, nil
}
