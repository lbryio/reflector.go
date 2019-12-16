package reflector

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/lbryio/lbry.go/v2/extras/stop"

	log "github.com/sirupsen/logrus"
)

type increment int

const (
	sdInc increment = iota + 1
	blobInc
	errInc
)

type Summary struct {
	Total, AlreadyStored, Sd, Blob, Err int
}

type Uploader struct {
	db              *db.SQL
	store           *store.DBBackedStore // could just be store.BlobStore interface
	workers         int
	skipExistsCheck bool
	stopper         *stop.Group
	countChan       chan increment

	count Summary
}

func NewUploader(db *db.SQL, store *store.DBBackedStore, workers int, skipExistsCheck bool) *Uploader {
	return &Uploader{
		db:              db,
		store:           store,
		workers:         workers,
		skipExistsCheck: skipExistsCheck,
		stopper:         stop.New(),
		countChan:       make(chan increment),
	}
}

func (u *Uploader) Stop() {
	log.Infoln("stopping uploader")
	u.stopper.StopAndWait()
}

func (u *Uploader) Upload(dirOrFilePath string) error {
	paths, err := getPaths(dirOrFilePath)
	if err != nil {
		return err
	}

	u.count.Total = len(paths)

	hashes := make([]string, len(paths))
	for i, p := range paths {
		hashes[i] = path.Base(p)
	}

	log.Debug("checking for existing blobs")

	var exists map[string]bool
	if !u.skipExistsCheck {
		exists, err = u.db.HasBlobs(hashes)
		if err != nil {
			return err
		}
		u.count.AlreadyStored = len(exists)
	}

	log.Debugf("%d new blobs to upload", u.count.Total-u.count.AlreadyStored)

	workerWG := sync.WaitGroup{}
	pathChan := make(chan string)

	for i := 0; i < u.workers; i++ {
		workerWG.Add(1)
		go func(i int) {
			defer workerWG.Done()
			defer func(i int) { log.Debugf("worker %d quitting", i) }(i)
			u.worker(pathChan)
		}(i)
	}

	countWG := sync.WaitGroup{}
	countWG.Add(1)
	go func() {
		defer countWG.Done()
		u.counter()
	}()

Upload:
	for _, f := range paths {
		if exists != nil && exists[path.Base(f)] {
			continue
		}

		select {
		case pathChan <- f:
		case <-u.stopper.Ch():
			break Upload
		}
	}

	close(pathChan)
	workerWG.Wait()
	close(u.countChan)
	countWG.Wait()
	u.stopper.Stop()

	log.Debugf(
		"upload stats: %d blobs total, %d already stored, %d SD blobs uploaded, %d content blobs uploaded, %d errors",
		u.count.Total, u.count.AlreadyStored, u.count.Sd, u.count.Blob, u.count.Err,
	)
	return nil
}

// worker reads paths from a channel and uploads them
func (u *Uploader) worker(pathChan chan string) {
	for {
		select {
		case <-u.stopper.Ch():
			return
		case filepath, ok := <-pathChan:
			if !ok {
				return
			}

			err := u.uploadBlob(filepath)
			if err != nil {
				log.Errorln(err)
			}
		}
	}
}

// uploadBlob uploads a blob
func (u *Uploader) uploadBlob(filepath string) (err error) {
	defer func() {
		if err != nil {
			u.inc(errInc)
		}
	}()

	blob, err := ioutil.ReadFile(filepath)
	if err != nil {
		return err
	}

	hash := BlobHash(blob)
	if hash != path.Base(filepath) {
		return errors.Err("file name does not match hash (%s != %s), skipping", filepath, hash)
	}

	if IsValidJSON(blob) {
		log.Debugf("uploading SD blob %s", hash)
		err := u.store.PutSD(hash, blob)
		if err != nil {
			return errors.Prefix("uploading SD blob "+hash, err)
		}
		u.inc(sdInc)
	} else {
		log.Debugf("uploading blob %s", hash)
		err = u.store.Put(hash, blob)
		if err != nil {
			return errors.Prefix("uploading blob "+hash, err)
		}
		u.inc(blobInc)
	}

	return nil
}

// counter updates the counts of how many sd blobs and content blobs were uploaded, and how many
// errors were encountered. It occasionally prints the upload progress to debug.
func (u *Uploader) counter() {
	start := time.Now()

	for {
		select {
		case <-u.stopper.Ch():
			return
		case incrementType, ok := <-u.countChan:
			if !ok {
				return
			}
			switch incrementType {
			case sdInc:
				u.count.Sd++
			case blobInc:
				u.count.Blob++
			case errInc:
				u.count.Err++
			}
		}
		if (u.count.Sd+u.count.Blob)%50 == 0 {
			log.Debugf("%d of %d done (%s elapsed, %.3fs per blob)", u.count.Sd+u.count.Blob, u.count.Total-u.count.AlreadyStored, time.Since(start).String(), time.Since(start).Seconds()/float64(u.count.Sd+u.count.Blob))
		}
	}
}

func (u *Uploader) GetSummary() Summary {
	return u.count
}

func (u *Uploader) inc(t increment) {
	select {
	case u.countChan <- t:
	case <-u.stopper.Ch():
	}
}

// getPaths returns the paths for files to upload. it takes a path to a file or a dir. for a file,
// it returns the full path to that file. for a dir, it returns the paths for all the files in the
// dir
func getPaths(dirOrFilePath string) ([]string, error) {
	info, err := os.Stat(dirOrFilePath)
	if err != nil {
		return nil, errors.Err(err)
	}

	if info.Mode().IsRegular() {
		return []string{dirOrFilePath}, nil
	}

	f, err := os.Open(dirOrFilePath)
	if err != nil {
		return nil, errors.Err(err)
	}

	files, err := f.Readdir(-1)
	if err != nil {
		return nil, errors.Err(err)
	}
	err = f.Close()
	if err != nil {
		return nil, errors.Err(err)
	}

	var filenames []string
	for _, file := range files {
		if !file.IsDir() {
			filenames = append(filenames, dirOrFilePath+"/"+file.Name())
		}
	}

	return filenames, nil
}
