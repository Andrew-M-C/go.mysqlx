package mysqlx

import (
	"database/sql"

	"github.com/jmoiron/sqlx"
)

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

func (tx *tx) Delete(prototype interface{}, args ...interface{}) (sql.Result, error) {
	return tx.db.delete(tx.sqlx, prototype, args...)
}

func (tx *tx) Insert(v interface{}, opts ...Options) (sql.Result, error) {
	return tx.db.insert(tx.sqlx, v, opts...)
}

func (tx *tx) InsertIfNotExists(insert interface{}, conds ...interface{}) (sql.Result, error) {
	return tx.db.selectOrInsert(tx.sqlx, insert, nil, conds...)
}
func (tx *tx) InsertMany(records interface{}, opts ...Options) (result sql.Result, err error) {
	return tx.db.insertMany(tx.sqlx, records, opts...)
}

func (tx *tx) InsertOnDuplicateKeyUpdate(v interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error) {
	return tx.db.insertOnDuplicateKeyUpdate(tx.sqlx, v, updates, opts...)
}

func (tx *tx) InsertManyOnDuplicateKeyUpdate(records interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error) {
	return tx.db.insertManyOnDuplicateKeyUpdate(tx.sqlx, records, updates, opts...)
}

func (tx *tx) Select(dst interface{}, args ...interface{}) error {
	return tx.db.selectFunc(tx.sqlx, dst, args...)
}

func (tx *tx) SelectOrInsert(insert interface{}, selectResult interface{}, conds ...interface{}) (sql.Result, error) {
	return tx.db.selectOrInsert(tx.sqlx, insert, selectResult, conds...)
}

func (tx *tx) Update(prototype interface{}, fields map[string]interface{}, args ...interface{}) (sql.Result, error) {
	return tx.db.update(tx.sqlx, prototype, fields, args...)
}
