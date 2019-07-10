package reflector

import (
	"io/ioutil"
	"os"
	"path"
	"sync"
	"time"

	"github.com/lbryio/reflector.go/db"
	"github.com/lbryio/reflector.go/store"

	"github.com/lbryio/lbry.go/extras/errors"
	"github.com/lbryio/lbry.go/extras/stop"

	log "github.com/sirupsen/logrus"
)

type increment int

const (
	sdInc increment = iota + 1
	blobInc
	errInc
)

type Summary struct {
	total, alreadyStored, sd, blob, err int
}

type Uploader struct {
	db              *db.SQL
	store           *store.DBBackedS3Store // could just be store.BlobStore interface
	workers         int
	skipExistsCheck bool
	stopper         *stop.Group
	countChan       chan increment

	count Summary
}

func NewUploader(db *db.SQL, store *store.DBBackedS3Store, workers int, skipExistsCheck bool) *Uploader {
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

	u.count.total = len(paths)

	hashes := make([]string, len(paths))
	for i, p := range paths {
		hashes[i] = path.Base(p)
	}

	log.Infoln("checking for existing blobs")

	var exists map[string]bool
	if !u.skipExistsCheck {
		exists, err = u.db.HasBlobs(hashes)
		if err != nil {
			return err
		}
		u.count.alreadyStored = len(exists)
	}

	log.Infof("%d new blobs to upload", u.count.total-u.count.alreadyStored)

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

	log.Infoln("SUMMARY")
	log.Infof("%d blobs total", u.count.total)
	log.Infof("%d blobs already stored", u.count.alreadyStored)
	log.Infof("%d SD blobs uploaded", u.count.sd)
	log.Infof("%d content blobs uploaded", u.count.blob)
	log.Infof("%d errors encountered", u.count.err)

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
		log.Debugf("Uploading SD blob %s", hash)
		err := u.store.PutSD(hash, blob)
		if err != nil {
			return errors.Prefix("Uploading SD blob "+hash, err)
		}
		u.inc(sdInc)
	} else {
		log.Debugf("Uploading blob %s", hash)
		err = u.store.Put(hash, blob)
		if err != nil {
			return errors.Prefix("Uploading blob "+hash, err)
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
				u.count.sd++
			case blobInc:
				u.count.blob++
			case errInc:
				u.count.err++
			}
		}
		if (u.count.sd+u.count.blob)%50 == 0 {
			log.Infof("%d of %d done (%s elapsed, %.3fs per blob)", u.count.sd+u.count.blob, u.count.total-u.count.alreadyStored, time.Since(start).String(), time.Since(start).Seconds()/float64(u.count.sd+u.count.blob))
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
