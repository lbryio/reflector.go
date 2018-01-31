package db

import (
	"database/sql"

	"github.com/lbryio/errors.go"
	qtools "github.com/lbryio/query.go"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type DB interface {
	Connect(string) error
	AddBlob(string, int) error
	HasBlob(string) (bool, error)
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

func (s *SQL) AddBlob(hash string, length int) error {
	if s.conn == nil {
		return errors.Err("not connected")
	}

	if length <= 0 {
		return errors.Err("length must be positive")
	}

	query := "INSERT IGNORE INTO blobs (hash, length) VALUES (?,?)"
	args := []interface{}{hash, length}

	logQuery(query, args...)

	stmt, err := s.conn.Prepare(query)
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

	query := "SELECT EXISTS(SELECT 1 FROM blobs WHERE hash = ?)"
	args := []interface{}{hash}

	logQuery(query, args...)

	row := s.conn.QueryRow(query, args...)

	exists := false
	err := row.Scan(&exists)

	return exists, errors.Err(err)
}
