package db

import (
	"context"
	"database/sql"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/querytools"
	"github.com/lbryio/reflector.go/dht/bits"

	_ "github.com/go-sql-driver/mysql" // blank import for db driver
	log "github.com/sirupsen/logrus"
)

// SdBlob is a special blob that contains information on the rest of the blobs in the stream
type SdBlob struct {
	StreamName string `json:"stream_name"`
	Blobs      []struct {
		Length   int    `json:"length"`
		BlobNum  int    `json:"blob_num"`
		BlobHash string `json:"blob_hash,omitempty"`
		Iv       string `json:"iv"`
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
	s, err := querytools.InterpolateParams(query, args...)
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

	return errors.Err(s.conn.Ping())
}

// AddBlob adds a blob to the database.
func (s *SQL) AddBlob(hash string, length int, isStored bool) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	return withTx(s.conn, func(tx *sql.Tx) error {
		return addBlob(tx, hash, length, isStored)
	})
}

func addBlob(tx *sql.Tx, hash string, length int, isStored bool) error {
	if length <= 0 {
		return errors.Err("length must be positive")
	}

	err := execTx(tx,
		"INSERT INTO blob_ (hash, is_stored, length) VALUES (?,?,?) ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored))",
		[]interface{}{hash, isStored, length},
	)
	if err != nil {
		return errors.Err(err)
	}

	return nil
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
	maxBatchSize := 100
	doneIndex := 0

	for len(hashes) > doneIndex {
		sliceEnd := doneIndex + maxBatchSize
		if sliceEnd > len(hashes) {
			sliceEnd = len(hashes)
		}
		log.Debugf("getting hashes[%d:%d] of %d", doneIndex, sliceEnd, len(hashes))
		batch := hashes[doneIndex:sliceEnd]

		query := "SELECT hash FROM blob_ WHERE is_stored = ? && hash IN (" + querytools.Qs(len(batch)) + ")"
		args := make([]interface{}, len(batch)+1)
		args[0] = true
		for i := range batch {
			args[i+1] = batch[i]
		}

		logQuery(query, args...)

		err := func() error {
			rows, err := s.conn.Query(query, args...)
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
	args := []interface{}{hash}

	query := "DELETE FROM stream WHERE sd_hash = ?"
	logQuery(query, args...)
	_, err := s.conn.Exec(query, args...)
	if err != nil {
		return errors.Err(err)
	}

	query = "DELETE FROM blob_ WHERE hash = ?"
	logQuery(query, args...)
	_, err = s.conn.Exec(query, args...)
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

// AddSDBlob takes the SD Hash number of blobs and the set of blobs. In a single db tx it inserts the sdblob information
// into a stream, and inserts the associated blobs' information in the database. If a blob fails the transaction is
// rolled back and error(s) are returned.
func (s *SQL) AddSDBlob(sdHash string, sdBlobLength int, sdBlob SdBlob) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	return withTx(s.conn, func(tx *sql.Tx) error {
		// insert sd blob
		err := addBlob(tx, sdHash, sdBlobLength, true)
		if err != nil {
			return err
		}

		// insert stream
		err = execTx(tx,
			"INSERT IGNORE INTO stream (hash, sd_hash) VALUES (?,?)",
			[]interface{}{sdBlob.StreamHash, sdHash},
		)
		if err != nil {
			return errors.Err(err)
		}

		// insert content blobs and connect them to stream
		for _, contentBlob := range sdBlob.Blobs {
			if contentBlob.BlobHash == "" {
				// null terminator blob
				continue
			}

			err := addBlob(tx, contentBlob.BlobHash, contentBlob.Length, false)
			if err != nil {
				return err
			}

			err = execTx(tx,
				"INSERT IGNORE INTO stream_blob (stream_hash, blob_hash, num) VALUES (?,?,?)",
				[]interface{}{sdBlob.StreamHash, contentBlob.BlobHash, contentBlob.BlobNum},
			)
			if err != nil {
				return errors.Err(err)
			}
		}
		return nil
	})
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

func execTx(tx *sql.Tx, query string, args []interface{}) error {
	logQuery(query, args...)
	_, err := tx.Exec(query, args...)
	return errors.Err(err)
}

/*  SQL schema

CREATE TABLE blob_ (
  hash char(96) NOT NULL,
  is_stored TINYINT(1) NOT NULL DEFAULT 0,
  length bigint(20) unsigned DEFAULT NULL,
  PRIMARY KEY (hash)
);

CREATE TABLE stream (
  hash char(96) NOT NULL,
  sd_hash char(96) NOT NULL,
  PRIMARY KEY (hash),
  KEY sd_hash_idx (sd_hash),
  FOREIGN KEY (sd_hash) REFERENCES blob_ (hash) ON DELETE RESTRICT ON UPDATE CASCADE
);

CREATE TABLE stream_blob (
  stream_hash char(96) NOT NULL,
  blob_hash char(96) NOT NULL,
  num int NOT NULL,
  PRIMARY KEY (stream_hash, blob_hash),
  FOREIGN KEY (stream_hash) REFERENCES stream (hash) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (blob_hash) REFERENCES blob_ (hash) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE TABLE blocked (
  hash char(96) NOT NULL,
  PRIMARY KEY (hash)
);

could add UNIQUE KEY (stream_hash, num) to stream_blob ...

*/
