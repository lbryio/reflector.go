package db

import (
	"context"
	"database/sql"
	"strings"
	"time"

	"github.com/lbryio/lbry.go/v2/dht/bits"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	qt "github.com/lbryio/lbry.go/v2/extras/query"
	"github.com/lbryio/lbry.go/v2/stream"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql" // blank import for db driver ensures its imported even if its not used
	log "github.com/sirupsen/logrus"
	"github.com/volatiletech/null"
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

type trackAccess int

const (
	// Don't track accesses
	TrackAccessNone trackAccess = iota
	// Track accesses at the stream level
	TrackAccessStreams
	// Track accesses at the blob level
	TrackAccessBlobs
)

// SQL implements the DB interface
type SQL struct {
	conn *sql.DB

	// Track the approx last time a blob or stream was accessed
	TrackAccess trackAccess

	// Instead of deleting a blob, marked it as not stored in the db
	SoftDelete bool
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
func (s *SQL) AddBlob(hash string, length int, isStored bool) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	_, err := s.insertBlob(hash, length, isStored)
	return err
}

// AddBlob adds a blob to the database.
func (s *SQL) AddBlobs(hash []string) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}
	// Split the slice into batches of 20 items.
	batch := 10000

	for i := 0; i < len(hash); i += batch {
		j := i + batch
		if j > len(hash) {
			j = len(hash)
		}
		err := s.insertBlobs(hash[i:j]) // Process the batch.
		if err != nil {
			log.Errorf("error while inserting batch: %s", errors.FullTrace(err))
		}
	}
	return nil
}

func (s *SQL) insertBlobs(hashes []string) error {
	var (
		q    string
		args []interface{}
	)
	dayAgo := time.Now().AddDate(0, 0, -1)
	q = "insert into blob_ (hash, is_stored, length, last_accessed_at) values "
	for _, hash := range hashes {
		q += "(?,?,?,?),"
		args = append(args, hash, true, stream.MaxBlobSize, dayAgo)
	}
	q = strings.TrimSuffix(q, ",")
	_, err := s.exec(q, args...)
	if err != nil {
		return err
	}

	return nil
}

func (s *SQL) insertBlob(hash string, length int, isStored bool) (int64, error) {
	if length <= 0 {
		return 0, errors.Err("length must be positive")
	}

	var (
		q    string
		args []interface{}
	)
	if s.TrackAccess == TrackAccessBlobs {
		args = []interface{}{hash, isStored, length, time.Now()}
		q = "INSERT INTO blob_ (hash, is_stored, length, last_accessed_at) VALUES (" + qt.Qs(len(args)) + ") ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored)), last_accessed_at = VALUES(last_accessed_at)"
	} else {
		args = []interface{}{hash, isStored, length}
		q = "INSERT INTO blob_ (hash, is_stored, length) VALUES (" + qt.Qs(len(args)) + ") ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored))"
	}

	blobID, err := s.exec(q, args...)
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

		if s.TrackAccess == TrackAccessBlobs {
			err := s.touchBlobs([]uint64{uint64(blobID)})
			if err != nil {
				return 0, errors.Err(err)
			}
		}
	}

	return blobID, nil
}

func (s *SQL) insertStream(hash string, sdBlobID int64) (int64, error) {
	var (
		q    string
		args []interface{}
	)

	if s.TrackAccess == TrackAccessStreams {
		args = []interface{}{hash, sdBlobID, time.Now()}
		q = "INSERT IGNORE INTO stream (hash, sd_blob_id, last_accessed_at) VALUES (" + qt.Qs(len(args)) + ")"
	} else {
		args = []interface{}{hash, sdBlobID}
		q = "INSERT IGNORE INTO stream (hash, sd_blob_id) VALUES (" + qt.Qs(len(args)) + ")"
	}

	streamID, err := s.exec(q, args...)
	if err != nil {
		return 0, errors.Err(err)
	}

	if streamID == 0 {
		err = s.conn.QueryRow("SELECT id FROM stream WHERE sd_blob_id = ?", sdBlobID).Scan(&streamID)
		if err != nil {
			return 0, errors.Err(err)
		}
		if streamID == 0 {
			return 0, errors.Err("stream ID is 0 even after INSERTing and SELECTing")
		}

		if s.TrackAccess == TrackAccessStreams {
			err := s.touchStreams([]uint64{uint64(streamID)})
			if err != nil {
				return 0, errors.Err(err)
			}
		}
	}
	return streamID, nil
}

// HasBlob checks if the database contains the blob information.
func (s *SQL) HasBlob(hash string, touch bool) (bool, error) {
	exists, err := s.HasBlobs([]string{hash}, touch)
	if err != nil {
		return false, err
	}
	return exists[hash], nil
}

// HasBlobs checks if the database contains the set of blobs and returns a bool map.
func (s *SQL) HasBlobs(hashes []string, touch bool) (map[string]bool, error) {
	exists, idsNeedingTouch, err := s.hasBlobs(hashes)

	if touch {
		if s.TrackAccess == TrackAccessBlobs {
			s.touchBlobs(idsNeedingTouch)
		} else if s.TrackAccess == TrackAccessStreams {
			s.touchStreams(idsNeedingTouch)
		}
	}

	return exists, err
}

func (s *SQL) touchBlobs(blobIDs []uint64) error {
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
	log.Debugf("touched %d blobs and took %s", len(blobIDs), time.Since(startTime))
	return errors.Err(err)
}

func (s *SQL) touchStreams(streamIDs []uint64) error {
	if len(streamIDs) == 0 {
		return nil
	}

	query := "UPDATE stream SET last_accessed_at = ? WHERE id IN (" + qt.Qs(len(streamIDs)) + ")"
	args := make([]interface{}, len(streamIDs)+1)
	args[0] = time.Now()
	for i := range streamIDs {
		args[i+1] = streamIDs[i]
	}

	startTime := time.Now()
	_, err := s.exec(query, args...)
	log.Debugf("touched %d streams and took %s", len(streamIDs), time.Since(startTime))
	return errors.Err(err)
}

func (s *SQL) hasBlobs(hashes []string) (map[string]bool, []uint64, error) {
	if s.conn == nil {
		return nil, nil, errors.Err("not connected")
	}

	var (
		hash           string
		blobID         uint64
		streamID       null.Uint64
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

		var lastAccessedAtSelect string
		if s.TrackAccess == TrackAccessBlobs {
			lastAccessedAtSelect = "b.last_accessed_at"
		} else if s.TrackAccess == TrackAccessStreams {
			lastAccessedAtSelect = "s.last_accessed_at"
		} else {
			lastAccessedAtSelect = "NULL"
		}

		query := `SELECT b.hash, b.id, s.id, ` + lastAccessedAtSelect + `
FROM blob_ b
LEFT JOIN stream_blob sb ON b.id = sb.blob_id
LEFT JOIN stream s on (sb.stream_id = s.id or s.sd_blob_id = b.id)
WHERE b.is_stored = 1 and b.hash IN (` + qt.Qs(len(batch)) + `)`
		args := make([]interface{}, len(batch))
		for i := range batch {
			args[i] = batch[i]
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
				err := rows.Scan(&hash, &blobID, &streamID, &lastAccessedAt)
				if err != nil {
					return errors.Err(err)
				}
				exists[hash] = true
				if !lastAccessedAt.Valid || lastAccessedAt.Time.Before(touchDeadline) {
					if s.TrackAccess == TrackAccessBlobs {
						needsTouch = append(needsTouch, blobID)
					} else if s.TrackAccess == TrackAccessStreams && !streamID.IsZero() {
						needsTouch = append(needsTouch, streamID.Uint64)
					}
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

// Delete will remove (or soft-delete) the blob from the db
// NOTE: If SoftDelete is enabled, streams will never be deleted
func (s *SQL) Delete(hash string) error {
	if s.SoftDelete {
		_, err := s.exec("UPDATE blob_ SET is_stored = 0 WHERE hash = ?", hash)
		return errors.Err(err)
	}

	_, err := s.exec("DELETE FROM stream WHERE sd_blob_id = (SELECT id FROM blob_ WHERE hash = ?)", hash)
	if err != nil {
		return errors.Err(err)
	}

	_, err = s.exec("DELETE FROM blob_ WHERE hash = ?", hash)
	return errors.Err(err)
}

// GetHashRange gets the smallest and biggest hashes in the db
func (s *SQL) LeastRecentlyAccessedHashes(maxBlobs int) ([]string, error) {
	if s.conn == nil {
		return nil, errors.Err("not connected")
	}

	if s.TrackAccess != TrackAccessBlobs {
		return nil, errors.Err("blob access tracking is disabled")
	}

	query := "SELECT hash from blob_ where is_stored = 1 order by last_accessed_at limit ?"
	logQuery(query, maxBlobs)

	rows, err := s.conn.Query(query, maxBlobs)
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

// AllHashes writes all hashes from the db into the channel.
// It does not close the channel when it finishes.
//func (s *SQL) AllHashes(ch chan<- string) error {
//	if s.conn == nil {
//		return errors.Err("not connected")
//	}
//
//	query := "SELECT hash from blob_"
//	if s.SoftDelete {
//		query += " where is_stored = 1"
//	}
//	logQuery(query)
//
//	rows, err := s.conn.Query(query)
//	if err != nil {
//		return errors.Err(err)
//	}
//	defer closeRows(rows)
//
//	for rows.Next() {
//		var hash string
//		err := rows.Scan(&hash)
//		if err != nil {
//			return errors.Err(err)
//		}
//		ch <- hash
//      // TODO: this needs testing
//		// TODO: need a way to cancel this early (e.g. in case of shutdown)
//	}
//
//	close(ch)
//	return nil
//}

func (s *SQL) Count() (int, error) {
	if s.conn == nil {
		return 0, errors.Err("not connected")
	}

	query := "SELECT count(id) from blob_"
	if s.SoftDelete {
		query += " where is_stored = 1"
	}
	logQuery(query)

	var count int
	err := s.conn.QueryRow(query).Scan(&count)
	return count, errors.Err(err)
}

// Block will mark a blob as blocked
func (s *SQL) Block(hash string) error {
	query := "INSERT IGNORE INTO blocked SET hash = ?"
	args := []interface{}{hash}
	logQuery(query, args...)
	_, err := s.conn.Exec(query, args...)
	return errors.Err(err)
}

// GetBlocked will return a list of blocked hashes
func (s *SQL) GetBlocked() (map[string]bool, error) {
	query := "SELECT hash FROM blocked"
	logQuery(query)
	rows, err := s.conn.Query(query)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer closeRows(rows)

	blocked := make(map[string]bool)

	var hash string
	for rows.Next() {
		err := rows.Scan(&hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		blocked[hash] = true
	}

	err = rows.Err()
	if err != nil {
		return nil, errors.Err(err)
	}

	return blocked, nil
}

// MissingBlobsForKnownStream returns missing blobs for an existing stream
// WARNING: if the stream does NOT exist, no blob hashes will be returned, which looks
// like no blobs are missing
func (s *SQL) MissingBlobsForKnownStream(sdHash string) ([]string, error) {
	if s.conn == nil {
		return nil, errors.Err("not connected")
	}

	query := `
		SELECT b.hash FROM blob_ b
		INNER JOIN stream_blob sb ON b.id = sb.blob_id
		INNER JOIN stream s ON s.id = sb.stream_id
		INNER JOIN blob_ sdb ON sdb.id = s.sd_blob_id AND sdb.hash = ?
		WHERE b.is_stored = 0
	`
	args := []interface{}{sdHash}

	logQuery(query, args...)

	rows, err := s.conn.Query(query, args...)
	if err != nil {
		return nil, errors.Err(err)
	}
	defer closeRows(rows)

	var missingBlobs []string
	var hash string

	for rows.Next() {
		err := rows.Scan(&hash)
		if err != nil {
			return nil, errors.Err(err)
		}
		missingBlobs = append(missingBlobs, hash)
	}

	err = rows.Err()
	if err != nil {
		return nil, errors.Err(err)
	}

	return missingBlobs, errors.Err(err)
}

// AddSDBlob insert the SD blob and all the content blobs. The content blobs are marked as "not stored",
// but they are tracked so reflector knows what it is missing.
func (s *SQL) AddSDBlob(sdHash string, sdBlobLength int, sdBlob SdBlob) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	sdBlobID, err := s.insertBlob(sdHash, sdBlobLength, true)
	if err != nil {
		return err
	}

	streamID, err := s.insertStream(sdBlob.StreamHash, sdBlobID)
	if err != nil {
		return err
	}

	// insert content blobs and connect them to stream
	for _, contentBlob := range sdBlob.Blobs {
		if contentBlob.BlobHash == "" {
			// null terminator blob
			continue
		}

		blobID, err := s.insertBlob(contentBlob.BlobHash, contentBlob.Length, false)
		if err != nil {
			return err
		}

		args := []interface{}{streamID, blobID, contentBlob.BlobNum}
		_, err = s.exec(
			"INSERT IGNORE INTO stream_blob (stream_id, blob_id, num) VALUES ("+qt.Qs(len(args))+")",
			args...,
		)
		if err != nil {
			return errors.Err(err)
		}
	}
	return nil
}

// GetHashRange gets the smallest and biggest hashes in the db
func (s *SQL) GetHashRange() (string, string, error) {
	var min string
	var max string

	if s.conn == nil {
		return "", "", errors.Err("not connected")
	}

	query := "SELECT MIN(hash), MAX(hash) from blob_"

	logQuery(query)

	err := s.conn.QueryRow(query).Scan(&min, &max)
	return min, max, err
}

// GetStoredHashesInRange gets stored blobs with hashes in a given range, and sends the hashes into a channel
func (s *SQL) GetStoredHashesInRange(ctx context.Context, start, end bits.Bitmap) (ch chan bits.Bitmap, ech chan error) {
	ch = make(chan bits.Bitmap)
	ech = make(chan error)

	// TODO: needs waitgroup?
	go func() {
		defer close(ch)
		defer close(ech)

		if s.conn == nil {
			ech <- errors.Err("not connected")
			return
		}

		query := "SELECT hash FROM blob_ WHERE hash >= ? AND hash <= ? AND is_stored = 1"
		args := []interface{}{start.Hex(), end.Hex()}

		logQuery(query, args...)

		rows, err := s.conn.Query(query, args...)
		defer closeRows(rows)
		if err != nil {
			ech <- err
			return
		}

		var hash string
	ScanLoop:
		for rows.Next() {
			err := rows.Scan(&hash)
			if err != nil {
				ech <- err
				return
			}
			select {
			case <-ctx.Done():
				break ScanLoop
			case ch <- bits.FromHexP(hash):
			}
		}

		err = rows.Err()
		if err != nil {
			ech <- err
			return
		}
	}()

	return
}

// txFunc is a function that can be wrapped in a transaction
type txFunc func(tx *sql.Tx) error

// withTx wraps a function in an sql transaction. the transaction is committed if there's no error, or rolled back if there is one.
// if dbOrTx is an sql.DB, a new transaction is started
func withTx(dbOrTx interface{}, f txFunc) (err error) {
	var tx *sql.Tx

	switch t := dbOrTx.(type) {
	case *sql.Tx:
		tx = t
	case *sql.DB:
		tx, err = t.Begin()
		if err != nil {
			return err
		}
		defer func() {
			if p := recover(); p != nil {
				if rollBackError := tx.Rollback(); rollBackError != nil {
					log.Error("failed to rollback tx on panic - ", rollBackError)
				}
				panic(p)
			} else if err != nil {
				if rollBackError := tx.Rollback(); rollBackError != nil {
					log.Error("failed to rollback tx on panic - ", rollBackError)
				}
			} else {
				err = errors.Err(tx.Commit())
			}
		}()
	default:
		return errors.Err("db or tx required")
	}

	return f(tx)
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

in prod, set tx_isolation to READ-COMMITTED to improve db performance
make sure you use latin1 or utf8 charset, NOT utf8mb4. that's a waste of space.

todo: could add UNIQUE KEY (stream_hash, num) to stream_blob ...

CREATE TABLE blob_ (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
  hash char(96) NOT NULL,
  is_stored TINYINT(1) NOT NULL DEFAULT 0,
  length bigint(20) unsigned DEFAULT NULL,
  last_accessed_at TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY blob_hash_idx (hash),
  KEY `blob_last_accessed_idx` (`last_accessed_at`)
);

CREATE TABLE stream (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
  hash char(96) NOT NULL,
  sd_blob_id BIGINT UNSIGNED NOT NULL,
  last_accessed_at TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY stream_hash_idx (hash),
  KEY stream_sd_blob_id_idx (sd_blob_id),
  KEY last_accessed_at_idx (last_accessed_at),
  FOREIGN KEY (sd_blob_id) REFERENCES blob_ (id) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE stream_blob (
  stream_id BIGINT UNSIGNED NOT NULL,
  blob_id BIGINT UNSIGNED NOT NULL,
  num int NOT NULL,
  PRIMARY KEY (stream_id, blob_id),
  KEY stream_blob_blob_id_idx (blob_id),
  FOREIGN KEY (stream_id) REFERENCES stream (id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (blob_id) REFERENCES blob_ (id) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE blocked (
  hash char(96) NOT NULL,
  PRIMARY KEY (hash)
);

*/

//func (d *LiteDBBackedStore) selfClean() {
//	d.stopper.Add(1)
//	defer d.stopper.Done()
//	lastCleanup := time.Now()
//	const cleanupInterval = 10 * time.Second
//	for {
//		select {
//		case <-d.stopper.Ch():
//			log.Infoln("stopping self cleanup")
//			return
//		default:
//			time.Sleep(1 * time.Second)
//		}
//		if time.Since(lastCleanup) < cleanupInterval {
//			continue
//
//		blobsCount, err := d.db.BlobsCount()
//		if err != nil {
//			log.Errorf(errors.FullTrace(err))
//		}
//		if blobsCount >= d.maxItems {
//			itemsToDelete := blobsCount / 100 * 10
//			blobs, err := d.db.GetLRUBlobs(itemsToDelete)
//			if err != nil {
//				log.Errorf(errors.FullTrace(err))
//			}
//			for _, hash := range blobs {
//				select {
//				case <-d.stopper.Ch():
//					return
//				default:
//
//				}
//				err = d.Delete(hash)
//				if err != nil {
//					log.Errorf(errors.FullTrace(err))
//				}
//				metrics.CacheLRUEvictCount.With(metrics.CacheLabels(d.Name(), d.component)).Inc()
//			}
//		}
//		lastCleanup = time.Now()
//	}
//}
