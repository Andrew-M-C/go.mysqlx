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

	// Insert insert a given structure. auto-increment fields will be ignored.
	Insert(v interface{}, opts ...Options) (sql.Result, error)

	// InsertIfNotExists is the same as SelectOrInsert but lacking select statement
	InsertIfNotExists(insert interface{}, conds ...interface{}) (sql.Result, error)

	// InsertMany insert multiple records into table. If additional option with table name is not given,
	// mysqlx will use the FIRST table name in records for all.
	InsertMany(records interface{}, opts ...Options) (result sql.Result, err error)

	// InsertOnDuplicateKeyUpdate executes 'INSERT ... ON DUPLICATE KEY UPDATE ...' statements. This function is
	// a combination of Insert and Update, without WHERE conditions.
	InsertOnDuplicateKeyUpdate(v interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error)

	// InsertManyOnDuplicateKeyUpdate is similar with InsertOnDuplicateKeyUpdate, but insert mutiple records for onetime.
	InsertManyOnDuplicateKeyUpdate(records interface{}, updates map[string]interface{}, opts ...Options) (sql.Result, error)

	// Select execute a SQL select statement
	Select(dst interface{}, args ...interface{}) error

	// SelectOrInsert executes update-if-not-exists statement
	SelectOrInsert(insert interface{}, selectResult interface{}, conds ...interface{}) (sql.Result, error)

	// Delete executes SQL DELETE statement with given conditions
	Delete(prototype interface{}, args ...interface{}) (sql.Result, error)

	// Update execute UPDATE SQL statement with given structure and conditions
	Update(prototype interface{}, fields map[string]interface{}, args ...interface{}) (sql.Result, error)
}

// DB represent a connection
type DB interface {
	CURD

	// AutoCreateTable enables auto table creation. DB will check if table is created previously before each access
	// to the DB.
	//
	// Please do NOT invoke this unless you need to automatically create table in runtime.
	//
	// Note 1: if a table was created once by mysqlx.DB after it was initialized, it will be noted as "created" and
	// cached. Then mysqlx.DB will not check into MySQL DB again.
	//
	// Note 2: auto-table-creation will NOT be activated in Select() function!
	AutoCreateTable()

	// Begin start a transaction
	Begin() (Tx, error)

	// CreateOrAlterTableStatements returns 'CREATE TABLE ... IF NOT EXISTS ...' or 'ALTER TABLE ...' statements, but
	// will not execute them. If the table does not exists, 'CREATE TABLE ...' statement will be returned. If the table
	// exists and needs no alteration, an empty string slice would be returned. Otherwise, a string slice with 'ALTER
	// TABLE ...' statements would be returned.
	//
	// The returned exists identifies if the table exists in database.
	CreateOrAlterTableStatements(v interface{}, opts ...Options) (exists bool, statements []string, err error)

	// CreateTable creates a table if not exist. If the table exists, it will alter it if necessary
	CreateTable(v interface{}, opts ...Options) error

	// CurrentDatabase gets current operating database
	CurrentDatabase() (string, error)

	// Database returns database name in DB
	Database() string

	// MustCreateTable is same as CreateTable. But is panics if error.
	MustCreateTable(v interface{}, opts ...Options)

	// KeepAlive automatically keeps alive with database
	KeepAlive()

	// StopKeepAlive stops the keep-alive operation.
	StopKeepAlive()

	// ReadTableFields returns all fields in given table.
	ReadTableFields(table string) (ret []*Field, err error)

	// ReadTableIndexes returns all indexes and uniques of given table name.
	ReadTableIndexes(table string) (map[string]*Index, map[string]*Unique, error)

	// SelectFields returns all valid SQL fields in given structure.
	SelectFields(s interface{}) (string, error)

	// InsertFields return keys and values for inserting. Auto-increment fields will be ignored
	InsertFields(s interface{}, backQuoted bool) (keys []string, values []string, err error)

	// StructFields is the same as ReadStructFields.
	StructFields(s interface{}) (ret []*Field, err error)

	// ReadStructFields returns all valid SQL fields by given structure and will buffer it.
	ReadStructFields(s interface{}) (ret []*Field, err error)

	// Sqlx return the *sqlx.DB object.
	Sqlx() *sqlx.DB
}

// Tx represent a transaction
type Tx interface {
	CURD

	// Sqlx return the *sqlx.Tx object.
	Sqlx() *sqlx.Tx

	// Rollback rollback a transaction.
	Rollback() error

	// Commit commits a transaction.
	Commit() error
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
