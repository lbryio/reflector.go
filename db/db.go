package db

import (
	"database/sql"

	"github.com/lbryio/lbry.go/errors"
	qtools "github.com/lbryio/query.go"
	"github.com/lbryio/reflector.go/types"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type DB interface {
	Connect(string) error
	HasBlob(string) (bool, error)
	AddBlob(string, int, bool) error
	AddSDBlob(string, int, types.SdBlob) error
}

type SQL struct {
	conn *sql.DB
}

func logQuery(query string, args ...interface{}) {
	s, err := qtools.InterpolateParams(query, args...)
	if err != nil {
		log.Errorln(err)
	} else {
		log.Debugln(s)
	}
}

func (s *SQL) Connect(dsn string) error {
	var err error
	dsn += "?parseTime=1&collation=utf8mb4_unicode_ci"
	s.conn, err = sql.Open("mysql", dsn)
	if err != nil {
		return errors.Err(err)
	}

	return errors.Err(s.conn.Ping())
}

func (s *SQL) AddBlob(hash string, length int, stored bool) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	return withTx(s.conn, func(tx *sql.Tx) error {
		return addBlob(tx, hash, length, stored)
	})
}

func addBlob(tx *sql.Tx, hash string, length int, stored bool) error {
	if length <= 0 {
		return errors.Err("length must be positive")
	}

	query := "INSERT INTO blob_ (hash, stored, length) VALUES (?,?,?) ON DUPLICATE KEY UPDATE stored = (stored or VALUES(stored))"
	args := []interface{}{hash, stored, length}

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

func (s *SQL) HasBlob(hash string) (bool, error) {
	if s.conn == nil {
		return false, errors.Err("not connected")
	}

	query := "SELECT EXISTS(SELECT 1 FROM blob_ WHERE hash = ? AND stored = ?)"
	args := []interface{}{hash, true}

	logQuery(query, args...)

	row := s.conn.QueryRow(query, args...)

	exists := false
	err := row.Scan(&exists)

	return exists, errors.Err(err)
}

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
				tx.Rollback()
				panic(p)
			} else if err != nil {
				tx.Rollback()
			} else {
				err = errors.Err(tx.Commit())
			}
		}()
	default:
		return errors.Err("db or tx required")
	}

	return f(tx)
}

func schema() {
	_ = `
CREATE TABLE blob_ (
  hash char(96) NOT NULL,
  stored TINYINT(1) NOT NULL DEFAULT 0,
  length bigint(20) unsigned DEFAULT NULL,
  last_announced_at datetime DEFAULT NULL,
  PRIMARY KEY (hash),
  KEY last_announced_at_idx (last_announced_at)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE stream (
  hash char(96) NOT NULL,
  sd_hash char(96) NOT NULL,
  PRIMARY KEY (hash),
  KEY sd_hash_idx (sd_hash),
  FOREIGN KEY (sd_hash) REFERENCES blob_ (hash) ON DELETE RESTRICT ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE TABLE stream_blob (
  stream_hash char(96) NOT NULL,
  blob_hash char(96) NOT NULL,
  num int NOT NULL,
  PRIMARY KEY (stream_hash, blob_hash),
  FOREIGN KEY (stream_hash) REFERENCES stream (hash) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (blob_hash) REFERENCES blob_ (hash) ON DELETE CASCADE ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

`
}
