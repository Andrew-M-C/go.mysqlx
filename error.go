package mysqlx

import (
	"fmt"
)

type sqlIntf interface {
	Query() string
}

// Error is the error type identified by mysqlx
type Error struct {
	err string
	sql string
}

// Error returns error message of the error
func (e *Error) Error() string {
	return e.err
}

// String returns detail description of the error
func (e *Error) String() string {
	if "" == e.sql {
		return e.err
	}
	return fmt.Sprintf("%s, SQL: <%s>", e.err, e.sql)
}

// Query returns MySQL query statements stored in Error object
func (e *Error) Query() string {
	return e.sql
}

// Errorf generates an formatted error object
func Errorf(format string, args ...interface{}) *Error {
	err := fmt.Sprintf(format, args...)
	return &Error{
		err: err,
		sql: "",
	}
}

// newError returns an error with sql statements
func newError(err, sql string) *Error {
	return &Error{
		err: err,
		sql: sql,
	}
}

// GetQueryFromError fetch SQL query statements in returned error type by mysqlx.
func GetQueryFromError(e error) string {
	if query, ok := interface{}(e).(sqlIntf); ok {
		return query.Query()
	}
	return ""
}
