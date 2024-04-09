package lite_db

import (
	"database/sql"
	"time"

	"github.com/lbryio/lbry.go/v2/extras/errors"
	qt "github.com/lbryio/lbry.go/v2/extras/query"

	"github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
	"github.com/volatiletech/null/v8"
)

// SdBlob is a special blob that contains information on the rest of the blobs in the stream
type SdBlob struct {
	StreamName string `json:"stream_name"`
	Blobs      []struct {
		Length   int    `json:"length"`
		BlobNum  int    `json:"blob_num"`
		BlobHash string `json:"blob_hash,omitempty"`
		IV       string `json:"iv"`
	} `json:"blobs"`
	StreamType        string `json:"stream_type"`
	Key               string `json:"key"`
	SuggestedFileName string `json:"suggested_file_name"`
	StreamHash        string `json:"stream_hash"`
}

// SQL implements the DB interface
type SQL struct {
	conn *sql.DB

	TrackAccessTime bool
}

func logQuery(query string, args ...interface{}) {
	s, err := qt.InterpolateParams(query, args...)
	if err != nil {
		log.Errorln(err)
	} else {
		log.Debugln(s)
	}
}

// Connect will create a connection to the database
func (s *SQL) Connect(dsn string) error {
	var err error
	// interpolateParams is necessary. otherwise uploading a stream with thousands of blobs
	// will hit MySQL's max_prepared_stmt_count limit because the prepared statements are all
	// opened inside a transaction. closing them manually doesn't seem to help
	dsn += "?parseTime=1&collation=utf8mb4_unicode_ci&interpolateParams=1"
	s.conn, err = sql.Open("mysql", dsn)
	if err != nil {
		return errors.Err(err)
	}

	s.conn.SetMaxIdleConns(12)

	return errors.Err(s.conn.Ping())
}

// AddBlob adds a blob to the database.
func (s *SQL) AddBlob(hash string, length int) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	_, err := s.insertBlob(hash, length)
	return err
}

func (s *SQL) insertBlob(hash string, length int) (int64, error) {
	if length <= 0 {
		return 0, errors.Err("length must be positive")
	}
	const isStored = true
	now := time.Now()
	args := []interface{}{hash, isStored, length, now}
	blobID, err := s.exec(
		"INSERT INTO blob_ (hash, is_stored, length, last_accessed_at) VALUES ("+qt.Qs(len(args))+") ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored)), last_accessed_at=VALUES(last_accessed_at)",
		args...,
	)
	if err != nil {
		return 0, err
	}

	if blobID == 0 {
		err = s.conn.QueryRow("SELECT id FROM blob_ WHERE hash = ?", hash).Scan(&blobID)
		if err != nil {
			return 0, errors.Err(err)
		}
		if blobID == 0 {
			return 0, errors.Err("blob ID is 0 even after INSERTing and SELECTing")
		}
	}

	return blobID, nil
}

// HasBlob checks if the database contains the blob information.
func (s *SQL) HasBlob(hash string) (bool, error) {
	exists, err := s.HasBlobs([]string{hash})
	if err != nil {
		return false, err
	}
	return exists[hash], nil
}

// HasBlobs checks if the database contains the set of blobs and returns a bool map.
func (s *SQL) HasBlobs(hashes []string) (map[string]bool, error) {
	exists, streamsNeedingTouch, err := s.hasBlobs(hashes)
	_ = s.touch(streamsNeedingTouch)
	return exists, err
}

func (s *SQL) touch(blobIDs []uint64) error {
	if len(blobIDs) == 0 {
		return nil
	}

	query := "UPDATE blob_ SET last_accessed_at = ? WHERE id IN (" + qt.Qs(len(blobIDs)) + ")"
	args := make([]interface{}, len(blobIDs)+1)
	args[0] = time.Now()
	for i := range blobIDs {
		args[i+1] = blobIDs[i]
	}

	startTime := time.Now()
	_, err := s.exec(query, args...)
	log.Debugf("blobs access query touched %d blobs and took %s", len(blobIDs), time.Since(startTime))
	return errors.Err(err)
}

func (s *SQL) hasBlobs(hashes []string) (map[string]bool, []uint64, error) {
	if s.conn == nil {
		return nil, nil, errors.Err("not connected")
	}

	var (
		hash           string
		blobID         uint64
		lastAccessedAt null.Time
	)

	var needsTouch []uint64
	exists := make(map[string]bool)

	touchDeadline := time.Now().AddDate(0, 0, -1) // touch blob if last accessed before this time
	maxBatchSize := 10000
	doneIndex := 0

	for len(hashes) > doneIndex {
		sliceEnd := doneIndex + maxBatchSize
		if sliceEnd > len(hashes) {
			sliceEnd = len(hashes)
		}
		log.Debugf("getting hashes[%d:%d] of %d", doneIndex, sliceEnd, len(hashes))
		batch := hashes[doneIndex:sliceEnd]

		// TODO: this query doesn't work for SD blobs, which are not in the stream_blob table

		query := `SELECT hash, id, last_accessed_at
FROM blob_ 
WHERE is_stored = ? and hash IN (` + qt.Qs(len(batch)) + `)`
		args := make([]interface{}, len(batch)+1)
		args[0] = true
		for i := range batch {
			args[i+1] = batch[i]
		}

		logQuery(query, args...)

		err := func() error {
			startTime := time.Now()
			rows, err := s.conn.Query(query, args...)
			log.Debugf("hashes query took %s", time.Since(startTime))
			if err != nil {
				return errors.Err(err)
			}
			defer closeRows(rows)

			for rows.Next() {
				err := rows.Scan(&hash, &blobID, &lastAccessedAt)
				if err != nil {
					return errors.Err(err)
				}
				exists[hash] = true
				if s.TrackAccessTime && (!lastAccessedAt.Valid || lastAccessedAt.Time.Before(touchDeadline)) {
					needsTouch = append(needsTouch, blobID)
				}
			}

			err = rows.Err()
			if err != nil {
				return errors.Err(err)
			}

			doneIndex += len(batch)
			return nil
		}()
		if err != nil {
			return nil, nil, err
		}
	}

	return exists, needsTouch, nil
}

// Delete will remove the blob from the db
func (s *SQL) Delete(hash string) error {
	_, err := s.exec("UPDATE blob_ set is_stored = ? WHERE hash = ?", 0, hash)
	return errors.Err(err)
}

// AddSDBlob insert the SD blob and all the content blobs. The content blobs are marked as "not stored",
// but they are tracked so reflector knows what it is missing.
func (s *SQL) AddSDBlob(sdHash string, sdBlobLength int) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	_, err := s.insertBlob(sdHash, sdBlobLength)
	return err
}

// GetHashRange gets the smallest and biggest hashes in the db
func (s *SQL) GetLRUBlobs(maxBlobs int) ([]string, error) {
	if s.conn == nil {
		return nil, errors.Err("not connected")
	}

	query := "SELECT hash from blob_ where is_stored = ? order by last_accessed_at limit ?"
	const isStored = true
	logQuery(query, isStored, maxBlobs)
	rows, err := s.conn.Query(query, isStored, maxBlobs)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer closeRows(rows)
	blobs := make([]string, 0, maxBlobs)
	for rows.Next() {
		var hash string
		err := rows.Scan(&hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		blobs = append(blobs, hash)
	}
	return blobs, nil
}

func (s *SQL) AllBlobs() ([]string, error) {
	if s.conn == nil {
		return nil, errors.Err("not connected")
	}

	query := "SELECT hash from blob_ where is_stored = ?" //TODO: maybe sorting them makes more sense?
	const isStored = true
	logQuery(query, isStored)
	rows, err := s.conn.Query(query, isStored)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer closeRows(rows)
	totalBlobs, err := s.BlobsCount()
	if err != nil {
		return nil, err
	}
	blobs := make([]string, 0, totalBlobs)
	for rows.Next() {
		var hash string
		err := rows.Scan(&hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		blobs = append(blobs, hash)
	}
	return blobs, nil
}

func (s *SQL) BlobsCount() (int, error) {
	if s.conn == nil {
		return 0, errors.Err("not connected")
	}

	query := "SELECT count(id) from blob_ where is_stored = ?" //TODO: maybe sorting them makes more sense?
	const isStored = true
	logQuery(query, isStored)
	var count int
	err := s.conn.QueryRow(query, isStored).Scan(&count)
	return count, errors.Err(err)
}

func closeRows(rows *sql.Rows) {
	if rows != nil {
		err := rows.Close()
		if err != nil {
			log.Error("error closing rows: ", err)
		}
	}
}

func (s *SQL) exec(query string, args ...interface{}) (int64, error) {
	logQuery(query, args...)
	attempt, maxAttempts := 0, 3
Retry:
	attempt++
	result, err := s.conn.Exec(query, args...)
	if isLockTimeoutError(err) {
		if attempt <= maxAttempts {
			//Error 1205: Lock wait timeout exceeded; try restarting transaction
			goto Retry
		}
		err = errors.Prefix("Lock timeout for query "+query, err)
	}

	if err != nil {
		return 0, errors.Err(err)
	}

	lastID, err := result.LastInsertId()
	return lastID, errors.Err(err)
}

func isLockTimeoutError(err error) bool {
	e, ok := err.(*mysql.MySQLError)
	return ok && e != nil && e.Number == 1205
}

/*  SQL schema

in prod make sure you use latin1 or utf8 charset, NOT utf8mb4. that's a waste of space.

CREATE TABLE `blob_` (
  `id` bigint unsigned NOT NULL AUTO_INCREMENT,
  `hash` char(96) NOT NULL,
  `is_stored` tinyint(1) NOT NULL DEFAULT '0',
  `length` bigint unsigned DEFAULT NULL,
  `last_accessed_at` datetime DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `id` (`id`),
  UNIQUE KEY `blob_hash_idx` (`hash`),
  KEY `blob_last_accessed_idx` (`last_accessed_at`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1

*/
