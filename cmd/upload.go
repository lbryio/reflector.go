package cmd

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/peer"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/extras/stop"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var uploadWorkers int
var uploadSkipExistsCheck bool

const (
	sdInc   = 1
	blobInc = 2
	errInc  = 3
)

type uploaderParams struct {
	workerWG  *sync.WaitGroup
	counterWG *sync.WaitGroup
	stopper   *stop.Group
	pathChan  chan string
	countChan chan int
	sdCount   int
	blobCount int
	errCount  int
}

func init() {
	var cmd = &cobra.Command{
		Use:   "upload PATH",
		Short: "Upload blobs to S3",
		Args:  cobra.ExactArgs(1),
		Run:   uploadCmd,
	}
	cmd.PersistentFlags().IntVar(&uploadWorkers, "workers", 1, "How many worker threads to run at once")
	cmd.PersistentFlags().BoolVar(&uploadSkipExistsCheck, "skipExistsCheck", false, "Dont check if blobs exist before uploading")
	rootCmd.AddCommand(cmd)
}

func uploadCmd(cmd *cobra.Command, args []string) {
	startTime := time.Now()
	db := new(db.SQL)
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)

	params := uploaderParams{
		workerWG:  &sync.WaitGroup{},
		counterWG: &sync.WaitGroup{},
		pathChan:  make(chan string),
		countChan: make(chan int),
		stopper:   stop.New()}

	setInterrupt(params.stopper)

	paths, err := getPaths(args[0])
	checkErr(err)

	totalCount := len(paths)

	hashes := make([]string, len(paths))
	for i, p := range paths {
		hashes[i] = path.Base(p)
	}

	log.Println("checking for existing blobs")

	exists := make(map[string]bool)
	if !uploadSkipExistsCheck {
		exists, err = db.HasBlobs(hashes)
		checkErr(err)
	}
	existsCount := len(exists)

	log.Printf("%d new blobs to upload", totalCount-existsCount)

	startUploadWorkers(&params)
	params.counterWG.Add(1)
	go func() {
		defer params.counterWG.Done()
		runCountReceiver(&params, startTime, totalCount, existsCount)
	}()

Upload:
	for _, f := range paths {
		if exists[path.Base(f)] {
			continue
		}

		select {
		case params.pathChan <- f:
		case <-params.stopper.Ch():
			log.Warnln("Caught interrupt, quitting at first opportunity...")
			break Upload
		}
	}

	close(params.pathChan)
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
	err := db.Connect(globalConfig.DBConn)
	checkErr(err)

	s3 := store.NewS3BlobStore(globalConfig.AwsID, globalConfig.AwsSecret, globalConfig.BucketRegion, globalConfig.BucketName)
	return store.NewDBBackedS3Store(s3, db)
}

func setInterrupt(stopper *stop.Group) {
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interruptChan
		stopper.Stop()
	}()
}

func startUploadWorkers(params *uploaderParams) {
	for i := 0; i < uploadWorkers; i++ {
		params.workerWG.Add(1)
		go func(i int) {
			defer params.workerWG.Done()
			defer func(i int) {
				log.Printf("worker %d quitting", i)
			}(i)

			blobStore := newBlobStore()
			launchFileUploader(params, blobStore, i)
		}(i)
	}
}

func launchFileUploader(params *uploaderParams, blobStore *store.DBBackedS3Store, worker int) {
	for {
		select {
		case <-params.stopper.Ch():
			return
		case filepath, ok := <-params.pathChan:
			if !ok {
				return
			}

			blob, err := ioutil.ReadFile(filepath)
			checkErr(err)

			hash := peer.GetBlobHash(blob)
			if hash != path.Base(filepath) {
				log.Errorf("worker %d: file name does not match hash (%s != %s), skipping", worker, filepath, hash)
				select {
				case params.countChan <- errInc:
				case <-params.stopper.Ch():
				}
				continue
			}

			if isJSON(blob) {
				log.Printf("worker %d: PUTTING SD BLOB %s", worker, hash)
				err := blobStore.PutSD(hash, blob)
				if err != nil {
					log.Error("PutSD Error: ", err)
				}
				select {
				case params.countChan <- sdInc:
				case <-params.stopper.Ch():
				}
			} else {
				log.Printf("worker %d: putting %s", worker, hash)
				err = blobStore.Put(hash, blob)
				if err != nil {
					log.Error("put Blob Error: ", err)
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

func getPaths(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	if info.Mode().IsRegular() {
		return []string{path}, nil
	}

	f, err := os.Open(path)
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
			filenames = append(filenames, path+"/"+file.Name())
		}
	}

	return filenames, nil
}
