package mysqlx

import (
	"database/sql"
	"fmt"
	"strings"

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

// CURD interface declares supported MySQL CURD operations
type CURD interface {
	Delete(prototype interface{}, args ...interface{}) (sql.Result, error)
	Insert(v interface{}, opts ...Options) (sql.Result, error)
	InsertIfNotExists(insert interface{}, conds ...interface{}) (sql.Result, error)
	InsertMany(records interface{}, opts ...Options) (result sql.Result, err error)
	InsertOnDuplicateKeyUpdate(v interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error)
	InsertManyOnDuplicateKeyUpdate(records interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error)
	Select(dst interface{}, args ...interface{}) error
	SelectOrInsert(insert interface{}, selectResult interface{}, conds ...interface{}) (sql.Result, error)
	Update(prototype interface{}, fields map[string]interface{}, args ...interface{}) (sql.Result, error)
}

// DB represent a connection
type DB interface {
	CURD

	AutoCreateTable()
	Begin() (Tx, error)
	CreateOrAlterTableStatements(v interface{}, opts ...Options) (exists bool, statements []string, err error)
	CreateTable(v interface{}, opts ...Options) error
	CurrentDatabase() (string, error)
	Database() string
	InsertFields(s interface{}, backQuoted bool) (keys []string, values []string, err error)
	KeepAlive()
	MustCreateTable(v interface{}, opts ...Options)
	ReadStructFields(s interface{}) (ret []*Field, err error)
	ReadTableFields(table string) (ret []*Field, err error)
	ReadTableIndexes(table string) (map[string]*Index, map[string]*Unique, error)
	SelectFields(s interface{}) (string, error)
	Sqlx() *sqlx.DB
	StopKeepAlive()
	StructFields(s interface{}) (ret []*Field, err error)
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
	OnUpdate      string
	// private
	statement string
}

// Options identifies options and parameters for a structure
type Options struct {
	// TableName defines the table name of this object
	TableName string
	// TableDescption defines the description of the table, it is used in create table statement
	TableDescption string
	// Indexes defines the indexes of the table
	Indexes []Index
	// Uniques defines the uniques of the table
	Uniques []Unique
	// CreateTableParams defines additional variables in create table statements.
	// There are three default variaments, which could be replaced:
	// ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4
	CreateTableParams map[string]string
	// DoNotExec stop the actual database executing process if it is set as true. Instead, CURD
	// functions would return an Error object with SQL query statement. This could used for troubleshot.
	// Please use GetQueryFromError() function to get the query statement.
	DoNotExec bool
}

// Offset is for MySQL offset statement
type Offset int

// Limit is for MySQL limit statement
type Limit int

// Raw stores a raw MySQL query statement. In mysqlx operation, Raw type will added to sql query statement directly
// without any escaping.
//
// Currently only update fields supports Raw, like:
//
//     map[string]interface{}{"id": mysqlx.Raw("= id")}
type Raw string
