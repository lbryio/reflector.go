package db

import (
	"context"
	"database/sql"

	"github.com/lbryio/lbry.go/errors"
	"github.com/lbryio/lbry.go/querytools"
	"github.com/lbryio/reflector.go/dht/bits"
	"github.com/lbryio/reflector.go/types"
	// blank import for db driver
	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

// DB interface communicates to a backend database with a simple set of methods that supports tracking blobs that are
// used together with a BlobStore. The DB tracks pointers and the BlobStore stores the data.
type DB interface {
	Connect(string) error
	HasBlob(string) (bool, error)
	AddBlob(string, int, bool) error
	AddSDBlob(string, int, types.SdBlob) error
	HasFullStream(string) (bool, error)
}

// SQL is the container for the supporting MySQL database connection.
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
	dsn += "?parseTime=1&collation=utf8mb4_unicode_ci"
	s.conn, err = sql.Open("mysql", dsn)
	if err != nil {
		return errors.Err(err)
	}

	return errors.Err(s.conn.Ping())
}

// AddBlob adds a blobs information to the database.
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

	query := "INSERT INTO blob_ (hash, is_stored, length) VALUES (?,?,?) ON DUPLICATE KEY UPDATE is_stored = (is_stored or VALUES(is_stored))"
	args := []interface{}{hash, isStored, length}

	logQuery(query, args...)

	stmt, err := tx.Prepare(query)
	if err != nil {
		return errors.Err(err)
	}

	_, err = stmt.Exec(args...)
	if err != nil {
		return errors.Err(err)
	}

	return nil
}

// HasBlob checks if the database contains the blob information.
func (s *SQL) HasBlob(hash string) (bool, error) {
	if s.conn == nil {
		return false, errors.Err("not connected")
	}

	query := "SELECT EXISTS(SELECT 1 FROM blob_ WHERE hash = ? AND is_stored = ?)"
	args := []interface{}{hash, true}

	logQuery(query, args...)

	row := s.conn.QueryRow(query, args...)

	exists := false
	err := row.Scan(&exists)

	return exists, errors.Err(err)
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

		rows, err := s.conn.Query(query, args...)
		if err != nil {
			closeRows(rows)
			return exists, err
		}

		for rows.Next() {
			err := rows.Scan(&hash)
			if err != nil {
				closeRows(rows)
				return exists, err
			}
			exists[hash] = true
		}

		err = rows.Err()
		if err != nil {
			closeRows(rows)
			return exists, err
		}

		closeRows(rows)
		doneIndex += len(batch)
	}

	return exists, nil
}

// HasFullStream checks if the full stream has been uploaded (i.e. if we have the sd blob and all the content blobs)
func (s *SQL) HasFullStream(sdHash string) (bool, error) {
	if s.conn == nil {
		return false, errors.Err("not connected")
	}

	query := `SELECT EXISTS(
		SELECT 1 FROM stream s
		LEFT JOIN stream_blob sb ON s.hash = sb.stream_hash
		LEFT JOIN blob_ b ON b.hash = sb.blob_hash
		WHERE s.sd_hash = ?
		GROUP BY s.sd_hash
		HAVING min(b.is_stored = 1)
	);`
	args := []interface{}{sdHash}

	logQuery(query, args...)

	row := s.conn.QueryRow(query, args...)

	exists := false
	err := row.Scan(&exists)

	return exists, errors.Err(err)
}

// AddSDBlob takes the SD Hash number of blobs and the set of blobs. In a single db tx it inserts the sdblob information
// into a stream, and inserts the associated blobs' information in the database. If a blob fails the transaction is
// rolled back and error(s) are returned.
func (s *SQL) AddSDBlob(sdHash string, sdBlobLength int, sdBlob types.SdBlob) error {
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
		query := "INSERT IGNORE INTO stream (hash, sd_hash) VALUES (?,?)"
		args := []interface{}{sdBlob.StreamHash, sdHash}

		logQuery(query, args...)

		stmt, err := tx.Prepare(query)
		if err != nil {
			return errors.Err(err)
		}

		_, err = stmt.Exec(args...)
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

			query := "INSERT IGNORE INTO stream_blob (stream_hash, blob_hash, num) VALUES (?,?,?)"
			args := []interface{}{sdBlob.StreamHash, contentBlob.BlobHash, contentBlob.BlobNum}

			logQuery(query, args...)

			stmt, err := tx.Prepare(query)
			if err != nil {
				return errors.Err(err)
			}

			_, err = stmt.Exec(args...)
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

		//query := "SELECT hash FROM blob_ WHERE hash >= ? AND hash <= ? AND is_stored = 1"
		//args := []interface{}{start.Hex(), end.Hex()}
		query := "SELECT hash FROM blob_ WHERE hash IN (?,?) AND is_stored = 1"
		args := []interface{}{
			"6363e3ed8d32156aebbbe8c0dd077e7029c7cdaec58e08271aa35baa4250ec531129cb4f7a9ac9b7285dbb7ba375ab11",
			"89c5c3f9794b0b24a03406e3b74361edb9ae70828e4c133512fc75db0a2d312673cdd4e30eed37892a46692d2fe439f3",
		}

		logQuery(query, args...)

		rows, err := s.conn.Query(query, args...)
		defer closeRows(rows)
		if err != nil {
			ech <- err
			return
		}

		var hash string
		for rows.Next() {
			err := rows.Scan(&hash)
			if err != nil {
				ech <- err
				return
			}
			select {
			case <-ctx.Done():
				break
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

could add UNIQUE KEY (stream_hash, num) to stream_blob ...

*/
