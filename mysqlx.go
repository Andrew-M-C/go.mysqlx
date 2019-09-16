package mysqlx

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type dbInfo struct {
	Name sql.NullString `db:"database()"`
}

func (d *DB) Database() string {
	return d.param.DBName
}

func New(db *sqlx.DB) (ret *DB, err error) {
	if nil == db {
		return nil, fmt.Errorf("nil *sqlx.DB")
	}

	// check whether db was connected to a certain databases
	var db_list []dbInfo
	err = db.Select(&db_list, "SELECT database()")
	if err != nil {
		return nil, err
	}
	if nil == db_list || 0 == len(db_list) {
		return nil, fmt.Errorf("Cannot determine database name in sqlx")
	}
	if false == db_list[0].Name.Valid {
		return nil, fmt.Errorf("sqlx is not using any database")
	}

	ret = &DB{}
	ret.db = db
	ret.param.DBName = db_list[0].Name.String
	return
}

func Open(param Param) (ret *DB, err error) {
	// check param
	if "" == param.Host {
		param.Host = "localhost"
	}
	if param.Port <= 0 {
		param.Port = 3306
	}
	if "" == param.DBName {
		return nil, fmt.Errorf("DBName required")
	}

	param.User = strings.Replace(param.User, "@", "\\@", -1)
	param.Pass = strings.Replace(param.Pass, "@", "\\@", -1)
	param.User = strings.Replace(param.User, "'", "\\'", -1)
	param.Pass = strings.Replace(param.Pass, "'", "\\'", -1)

	ret = &DB{
		param: param,
	}

	var uri string
	if "" == param.Pass {
		uri = fmt.Sprintf(
			"'%s'@tcp(%s:%d)/%s?charset=utf8&parseTime=true",
			param.User, param.Host, param.Port, param.DBName,
		)
	} else {
		uri = fmt.Sprintf(
			"'%s':'%s'@tcp(%s:%d)/%s?charset=utf8&parseTime=true",
			param.User, param.Pass, param.Host, param.Port, param.DBName,
		)
	}

	ret.db, err = sqlx.Open("mysql", uri)
	if err != nil {
		return nil, err
	}

	// test whether we can read data content
	var db_list []dbInfo
	err = ret.db.Select(&db_list, "SELECT database()")
	if err != nil {
		return nil, err
	}
	if nil == db_list || 0 == len(db_list) {
		return nil, fmt.Errorf("Cannot determine database name in sqlx")
	}
	if false == db_list[0].Name.Valid {
		return nil, fmt.Errorf("sqlx is not using any database")
	}

	ret.db.SetMaxIdleConns(5)
	return
}

func keepAlive(d *DB) {
	atomic.StoreInt32(&d.isKeepingAlive, 1)
	defer atomic.StoreInt32(&d.isKeepingAlive, 0)

	for true {
		should_keep_alive := atomic.LoadInt32(&d.shouldKeepAlive)
		if should_keep_alive <= 0 {
			return
		}

		time.Sleep(10 * time.Second)

		var res []interface{}
		err := d.db.Select(&res, "show tables")
		if err != nil {
			log.Printf("keeping alive failed: %v", err)
			return
		}
	}
	return
}

func (d *DB) Sqlx() *sqlx.DB {
	return d.db
}

func (d *DB) KeepAlive() {
	is_keeping_alive := atomic.LoadInt32(&d.isKeepingAlive)
	if is_keeping_alive > 0 {
		return
	}

	atomic.StoreInt32(&d.shouldKeepAlive, 1)
	go keepAlive(d)
	return
}

func (d *DB) StopKeepAlive() {
	atomic.StoreInt32(&d.shouldKeepAlive, 0)
}
