package mysqlx

import (
	"github.com/jmoiron/sqlx"
)

// Tx represent a transaction
type Tx interface {
	// CURD

	Sqlx() *sqlx.Tx
	Rollback() error
	Commit() error
}

type tx struct {
	sqlx *sqlx.Tx
	db   *xdb
}

// Begin create a transaction
func (db *xdb) Begin() (Tx, error) {
	sqlxTx, err := db.Sqlx().Beginx()
	if err != nil {
		return nil, err
	}

	return &tx{
		sqlx: sqlxTx,
		db:   db,
	}, nil
}

func (tx *tx) Sqlx() *sqlx.Tx {
	return tx.sqlx
}

func (tx *tx) Rollback() error {
	return tx.sqlx.Rollback()
}

func (tx *tx) Commit() error {
	return tx.sqlx.Commit()
}
