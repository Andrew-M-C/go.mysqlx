package mysqlx

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/syncmap"
)

type Param struct {
	Host   string
	Port   int
	User   string
	Pass   string
	DBName string
}

type DB struct {
	db    *sqlx.DB
	param Param

	// keep alive routine status
	shouldKeepAlive int32
	isKeepingAlive  int32

	// interface field buffers
	bufferedFields       syncmap.Map // []*Field
	bufferedFieldMaps    syncmap.Map // map[string]*Field
	bufferedSelectFields syncmap.Map // []string
}

type Index struct {
	Name   string
	Fields []string
}

func (i *Index) Check() error {
	if nil == i.Fields || 0 == len(i.Fields) {
		return fmt.Errorf("nil fields")
	}

	if "" == i.Name {
		i.Name = "index_" + strings.Join(i.Fields, "_")
	}

	return nil
}

type Unique struct {
	Name   string
	Fields []string
}

func (u *Unique) Check() error {
	if nil == u.Fields || 0 == len(u.Fields) {
		return fmt.Errorf("nil fields")
	}

	if "" == u.Name {
		u.Name = "uniq_" + strings.Join(u.Fields, "_")
	}

	return nil
}

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

type Options struct {
	TableName      string
	TableDescption string
	Indexes        []Index
	Uniques        []Unique
}

// MySQL statement tools
type Page struct {
	Offset int
	Limit  int
}

type Cond struct {
	Param    string
	Operator string
	Value    interface{}
}

type Order struct {
	Param string
	Seq   string
}
