package mysqlx

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	atomicbool "github.com/Andrew-M-C/go.atomicbool"

	// import mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

type dbInfo struct {
	Name sql.NullString `db:"database()"`
}

type sqlObj interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(dest interface{}, query string, args ...interface{}) error
}

// xdb is the main structure for mysqlx
type xdb struct {
	db    *sqlx.DB
	param Param

	// keep alive routine status
	shouldKeepAlive int32
	isKeepingAlive  int32

	// interface field buffers
	bufferedFields       sync.Map // []*Field
	bufferedFieldMaps    sync.Map // map[string]*Field
	bufferedSelectFields sync.Map // []string
	bufferedIncrField    sync.Map // *Field

	// stores created tables
	autoCreateTable atomicbool.B
	createdTables   sync.Map // bool
}

// Database returns database name in DB
func (d *xdb) Database() string {
	return d.param.DBName
}

// New initialize a DB object with a given *sqlx.DB, which should be connect a certain database.
func New(db *sqlx.DB) (DB, error) {
	if nil == db {
		return nil, fmt.Errorf("nil *sqlx.DB")
	}

	// check whether db was connected to a certain databases
	var err error
	var dbList []dbInfo
	err = db.Select(&dbList, "SELECT database()")
	if err != nil {
		return nil, err
	}
	if nil == dbList || 0 == len(dbList) {
		return nil, fmt.Errorf("Cannot determine database name in sqlx")
	}
	if !dbList[0].Name.Valid {
		return nil, fmt.Errorf("sqlx is not using any database")
	}

	ret := &xdb{}
	ret.db = db
	ret.param.DBName = dbList[0].Name.String
	return ret, nil
}

// SetDebugFunc set a function for debugging mysqlx
func SetDebugFunc(f func(string, ...any)) {
	if f != nil {
		internal.debugf = f
	}
}

// Open initialize a *xdb object with a valid *sqlx.DB
func Open(param Param) (DB, error) {
	var err error
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

	// param.User = strings.Replace(param.User, "@", "\\@", -1)
	// param.Pass = strings.Replace(param.Pass, "@", "\\@", -1)
	// param.User = strings.Replace(param.User, "'", "\\'", -1)
	// param.Pass = strings.Replace(param.Pass, "'", "\\'", -1)

	uri := genURI(&param)
	internal.debugf("Got URI: '%s'", uri)

	ret := &xdb{
		param: param,
	}
	ret.db, err = sqlx.Open("mysql", uri)
	if err != nil {
		return nil, err
	}

	// test whether we can read data content
	var dbList []dbInfo
	err = ret.db.Select(&dbList, "SELECT database()")
	if err != nil {
		err = fmt.Errorf("db.Select error: %w", err)
		return nil, err
	}
	if nil == dbList || 0 == len(dbList) {
		return nil, fmt.Errorf("Cannot determine database name in sqlx")
	}
	if !dbList[0].Name.Valid {
		return nil, fmt.Errorf("sqlx is not using any database")
	}

	ret.db.SetMaxIdleConns(5)
	return ret, nil
}

func genURI(p *Param) string {
	params := map[string]string{
		"charset":   "utf8mb4",
		"parseTime": "true",
	}
	for k, v := range p.Params {
		params[k] = v
	}

	p.Params = params
	buff := strings.Builder{}

	if p.Pass == "" {
		leading := fmt.Sprintf("%s@tcp(%s:%d)/%s?", p.User, p.Host, p.Port, p.DBName)
		buff.WriteString(leading)
	} else {
		leading := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?", p.User, p.Pass, p.Host, p.Port, p.DBName)
		buff.WriteString(leading)
	}

	firstKeyWritten := false
	for k, v := range params {
		if !firstKeyWritten {
			firstKeyWritten = true
		} else {
			buff.WriteByte('&')
		}

		buff.WriteString(k)
		buff.WriteByte('=')
		buff.WriteString(v)
	}

	return buff.String()
}

func keepAlive(d *xdb) {
	atomic.StoreInt32(&d.isKeepingAlive, 1)
	defer atomic.StoreInt32(&d.isKeepingAlive, 0)

	for {
		shouldKeepAlive := atomic.LoadInt32(&d.shouldKeepAlive)
		if shouldKeepAlive <= 0 {
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
}

// Sqlx return the *sqlx.DB object
func (d *xdb) Sqlx() *sqlx.DB {
	return d.db
}

// KeepAlive automatically keeps alive with database
func (d *xdb) KeepAlive() {
	isKeepingAlive := atomic.LoadInt32(&d.isKeepingAlive)
	if isKeepingAlive > 0 {
		return
	}

	atomic.StoreInt32(&d.shouldKeepAlive, 1)
	go keepAlive(d)
	return
}

// StopKeepAlive stops the keep-alive operation
func (d *xdb) StopKeepAlive() {
	atomic.StoreInt32(&d.shouldKeepAlive, 0)
}
