package db

import (
	"database/sql"

	"github.com/lbryio/lbry.go/v2/extras/errors"
)

// Executor can perform SQL queries.
type Executor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Transactor can commit and rollback, on top of being able to execute queries.
type Transactor interface {
	Commit() error
	Rollback() error

	Executor
}

// Begin begins a transaction
func Begin(db interface{}) (Transactor, error) {
	type beginner interface {
		Begin() (Transactor, error)
	}

	creator, ok := db.(beginner)
	if ok {
		return creator.Begin()
	}

	type sqlBeginner interface {
		Begin() (*sql.Tx, error)
	}

	creator2, ok := db.(sqlBeginner)
	if ok {
		return creator2.Begin()
	}

	return nil, errors.Err("database does not support transactions")
}
