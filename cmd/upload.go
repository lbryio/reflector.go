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

	stopper := stopOnce.New()
	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-interruptChan
		stopper.Stop()
	}()

	dir := args[0]

	f, err := os.Open(dir)
	checkErr(err)

	files, err := f.Readdir(-1)
	checkErr(err)
	err = f.Close()
	checkErr(err)

	var filenames []string
	for _, file := range files {
		if !file.IsDir() {
			filenames = append(filenames, file.Name())
		}
	}

	totalCount := len(filenames)

	log.Println("checking for existing blobs")

	exists, err := db.HasBlobs(filenames)
	checkErr(err)
	existsCount := len(exists)

	log.Printf("%d new blobs to upload", totalCount-existsCount)

	sdCount := 0
	blobCount := 0

	workerWG := &sync.WaitGroup{}
	filenameChan := make(chan string)
	counterWG := &sync.WaitGroup{}
	countChan := make(chan bool)

	for i := 0; i < workers; i++ {
		go func(i int) {
			workerWG.Add(1)
			defer workerWG.Done()
			defer func(i int) {
				log.Printf("worker %d quitting", i)
			}(i)

			blobStore := newBlobStore()

			for {
				select {
				case <-stopper.Chan():
					return
				case filename, ok := <-filenameChan:
					if !ok {
						return
					}

					blob, err := ioutil.ReadFile(dir + "/" + filename)
					checkErr(err)

					hash := peer.GetBlobHash(blob)
					if hash != filename {
						log.Errorf("worker %d: filename does not match hash (%s != %s), skipping", i, filename, hash)
						continue
					}

					if isJSON(blob) {
						log.Printf("worker %d: PUTTING SD BLOB %s", i, hash)
						blobStore.PutSD(hash, blob)
						select {
						case countChan <- true:
						case <-stopper.Chan():
						}
					} else {
						log.Printf("worker %d: putting %s", i, hash)
						blobStore.Put(hash, blob)
						select {
						case countChan <- false:
						case <-stopper.Chan():
						}
					}
				}
			}
		}(i)
	}

	go func() {
		counterWG.Add(1)
		defer counterWG.Done()
		for {
			select {
			case <-stopper.Chan():
				return
			case isSD, ok := <-countChan:
				if !ok {
					return
				} else if isSD {
					sdCount++
				} else {
					blobCount++
				}
			}
			if (sdCount+blobCount)%50 == 0 {
				log.Printf("%d of %d done (%s elapsed, %.2fs per blob)", sdCount+blobCount, totalCount-existsCount, time.Now().Sub(startTime).String(), time.Now().Sub(startTime).Seconds()/float64(sdCount+blobCount))
			}
		}
	}()

Upload:
	for _, filename := range filenames {
		if exists[filename] {
			continue
		}

		select {
		case filenameChan <- filename:
		case <-stopper.Chan():
			log.Warnln("Caught interrupt, quitting at first opportunity...")
			break Upload
		}
	}

	close(filenameChan)
	workerWG.Wait()
	close(countChan)
	counterWG.Wait()
	stopper.Stop()

	log.Println("SUMMARY")
	log.Printf("%d blobs total", totalCount)
	log.Printf("%d SD blobs uploaded", sdCount)
	log.Printf("%d content blobs uploaded", blobCount)
	log.Printf("%d blobs already stored", existsCount)
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
