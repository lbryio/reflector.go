package db

import (
	"context"
	"database/sql"
	"time"

	"github.com/lbryio/lbry.go/dht/bits"
	"github.com/lbryio/lbry.go/extras/errors"
	qt "github.com/lbryio/lbry.go/extras/query"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql" // blank import for db driver ensures its imported even if its not used
	log "github.com/sirupsen/logrus"
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

func (s *SQL) insertBlob(hash string, length int, isStored bool) (int64, error) {
	if length <= 0 {
		return 0, errors.Err("length must be positive")
	}

	blobID, err := s.exec(
		"INSERT INTO blob_ (hash, is_stored, length) VALUES (?,?,?) ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored))",
		hash, isStored, length,
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

func (s *SQL) insertStream(hash, sdHash string, sdBlobID int64) (int64, error) {
	streamID, err := s.exec(
		"INSERT IGNORE INTO stream (hash, sd_hash, sd_blob_id) VALUES (?,?, ?)",
		hash, sdHash, sdBlobID,
	)
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
	}
	return streamID, nil
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
	if s.conn == nil {
		return nil, errors.Err("not connected")
	}

	var hash string
	exists := make(map[string]bool)
	maxBatchSize := 10000
	doneIndex := 0

	for len(hashes) > doneIndex {
		sliceEnd := doneIndex + maxBatchSize
		if sliceEnd > len(hashes) {
			sliceEnd = len(hashes)
		}
		log.Debugf("getting hashes[%d:%d] of %d", doneIndex, sliceEnd, len(hashes))
		batch := hashes[doneIndex:sliceEnd]

		query := "SELECT hash FROM blob_ WHERE is_stored = ? && hash IN (" + qt.Qs(len(batch)) + ")"
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
				err := rows.Scan(&hash)
				if err != nil {
					return errors.Err(err)
				}
				exists[hash] = true
			}

			err = rows.Err()
			if err != nil {
				return errors.Err(err)
			}

			doneIndex += len(batch)
			return nil
		}()
		if err != nil {
			return nil, err
		}
	}

	return exists, nil
}

// Delete will remove the blob from the db
func (s *SQL) Delete(hash string) error {
	_, err := s.exec("DELETE FROM stream WHERE sd_hash = ?", hash)
	if err != nil {
		return errors.Err(err)
	}

	_, err = s.exec("DELETE FROM blob_ WHERE hash = ?", hash)
	return errors.Err(err)
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
		INNER JOIN stream_blob sb ON b.hash = sb.blob_hash
		INNER JOIN stream s ON s.hash = sb.stream_hash AND s.sd_hash = ?
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

	streamID, err := s.insertStream(sdBlob.StreamHash, sdHash, sdBlobID)
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

		_, err = s.exec(
			"INSERT IGNORE INTO stream_blob (stream_id, stream_hash, blob_id, blob_hash, num) VALUES (?,?,?,?,?)",
			streamID, sdBlob.StreamHash, blobID, contentBlob.BlobHash, contentBlob.BlobNum,
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
  PRIMARY KEY (id),
  UNIQUE KEY blob_hash_idx (hash)
);

CREATE TABLE stream (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
  hash char(96) NOT NULL,
  sd_blob_id BIGINT UNSIGNED NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY stream_hash_idx (hash),
  KEY stream_sd_blob_id_idx (sd_blob_id),
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


FOR THE MIGRATION, USE THESE TRIGGERS

CREATE TRIGGER tgr_stream_insert AFTER INSERT ON stream FOR EACH ROW INSERT INTO stream_new SET id = NEW.id, hash = NEW.hash, sd_blob_id = (SELECT id from blob_ where hash = NEW.sd_hash);
CREATE TRIGGER tgr_stream_delete AFTER DELETE ON stream FOR EACH ROW DELETE FROM stream_new WHERE id = OLD.id;

CREATE TRIGGER tgr_blob_insert AFTER INSERT ON blob_ FOR EACH ROW INSERT INTO blob_new SET id = NEW.id, hash = NEW.hash, is_stored = NEW.is_stored, length = NEW.length;
CREATE TRIGGER tgr_blob_update AFTER UPDATE ON blob_ FOR EACH ROW UPDATE blob_new SET hash = NEW.hash, is_stored = NEW.is_stored, length = NEW.length WHERE id = NEW.id;
CREATE TRIGGER tgr_blob_delete AFTER DELETE ON blob_ FOR EACH ROW DELETE FROM blob_new WHERE id = OLD.id;

CREATE TRIGGER tgr_stream_blob_insert AFTER INSERT ON stream_blob FOR EACH ROW INSERT INTO stream_blob_new SET stream_id = NEW.stream_id, blob_id = NEW.blob_id, num = NEW.num;
CREATE TRIGGER tgr_stream_blob_delete AFTER DELETE ON stream_blob FOR EACH ROW DELETE FROM stream_blob_new WHERE stream_id = OLD.stream_id AND blob_id = OLD.blob_id;



DROP PROCEDURE IF EXISTS copyblobs;
DELIMITER $$
CREATE PROCEDURE copyblobs()
BEGIN
  DECLARE first_trigger_blob_id INT DEFAULT 124802284; # ID of first blob that was copied using the triggers. dont copy anything after that.
  DECLARE i INT DEFAULT 0;
  DECLARE minid BIGINT UNSIGNED DEFAULT 0;
  SELECT min(id) INTO minid FROM blob_ WHERE id < first_trigger_blob_id AND id > (SELECT coalesce(max(id),0) from blob_new where id < first_trigger_blob_id);
  wloop: WHILE minid is not null DO
    #IF (i >= 100) THEN
    #  LEAVE wloop;
    #END IF;
    SET i = i + 1;
    IF (i % 5000 = 0) THEN
      SELECT concat('loop ', i, ', id ', minid) as progress;
    END IF;


    IF (i % 10 = 1) THEN  # we start our loops on 1, like normal people
      START TRANSACTION;
    END IF;

    INSERT INTO blob_new (id, hash, is_stored, length) SELECT id, hash, is_stored, length from blob_ where id = minid;

    IF (i % 10 = 0) THEN
      COMMIT;
    END IF;

    SELECT min(id) INTO minid FROM blob_ WHERE id < first_trigger_blob_id AND id > minid;
  END WHILE wloop;
  COMMIT;
END$$
DELIMITER ;



DROP PROCEDURE IF EXISTS copystreams;
DELIMITER $$
CREATE PROCEDURE copystreams()
BEGIN
  DECLARE first_trigger_stream_id INT DEFAULT 1465749; # ID of first stream that was copied using the triggers. dont copy anything after that.
  DECLARE i INT DEFAULT 0;
  DECLARE minid BIGINT UNSIGNED DEFAULT 0;
  DECLARE streamhash char(96);
  SELECT min(id) INTO minid FROM stream WHERE id < first_trigger_stream_id AND id > (SELECT coalesce(max(id),0) from stream_new where id < first_trigger_stream_id);
  wloop: WHILE minid is not null DO
    #IF (i >= 10) THEN
    #  LEAVE wloop;
    #END IF;
    SET i = i + 1;

    IF (i % 5000 = 0) THEN
      SELECT concat('loop ', i, ', id ', minid) as progress;
    END IF;

    IF (i % 10 = 1) THEN  # we start our loops on 1, like normal people
      START TRANSACTION;
    END IF;

	SELECT hash INTO streamhash FROM stream WHERE id = minid;
    INSERT INTO stream_new (id, hash, sd_blob_id) SELECT s.id, s.hash, b.id FROM stream s INNER JOIN blob_ b ON s.sd_hash = b.hash WHERE s.id = minid;
    INSERT INTO stream_blob_new SELECT minid, b.id, sb.num FROM stream_blob sb INNER JOIN blob_ b ON sb.blob_hash = b.hash WHERE sb.stream_hash = streamhash;

    IF (i % 10 = 0) THEN
      COMMIT;
    END IF;

    SELECT min(id) INTO minid FROM stream WHERE id < first_trigger_stream_id AND id > minid;
  END WHILE wloop;
  COMMIT;
END$$
DELIMITER ;


*/
