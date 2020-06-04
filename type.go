package mysqlx

import (
	"fmt"
	"strings"
	"sync"

	atomicbool "github.com/Andrew-M-C/go.atomicbool"
	"github.com/jmoiron/sqlx"
)

// Param identifies connect parameters to a database
type Param struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBName string
}

// RawStatement identifies raw MySQL statement, which will be directly added into sql statements.
// Now RawStatement is available in Update function.
type RawStatement string

// DB is the main structure for mysqlx
type DB struct {
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

// Index shows the information of an index setting
type Index struct {
	Name   string
	Fields []string
}

// Check checks if an index object is valid
func (i *Index) Check() error {
	if nil == i.Fields || 0 == len(i.Fields) {
		return fmt.Errorf("nil fields")
	}

	if "" == i.Name {
		i.Name = "index_" + strings.Join(i.Fields, "_")
	}

	return nil
}

// Unique shows the information of a unique setting
type Unique struct {
	Name   string
	Fields []string
}

// Check checks if an unique object is valid
func (u *Unique) Check() error {
	if nil == u.Fields || 0 == len(u.Fields) {
		return fmt.Errorf("nil fields")
	}

	if "" == u.Name {
		u.Name = "uniq_" + strings.Join(u.Fields, "_")
	}

	return nil
}

// Field shows information of a field
type Field struct {
	Name          string
	Type          string
	Nullable      bool
	Default       string
	Comment       string
	AutoIncrement bool
	// private
	statement string
}

// Options identifies options and parameters for a structure
type Options struct {
	TableName         string
	TableDescption    string
	Indexes           []Index
	Uniques           []Unique
	CreateTableParams map[string]string
}

// Offset is for MySQL offset statement
type Offset struct {
	Offset int
}

// Limit is for MySQL limit statement
type Limit struct {
	Limit int
}
