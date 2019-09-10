package mysqlx

import (
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestOpen(t *testing.T) {
	// successfully open
	db, err := Open(Param{
		DBName: "db_test",
	})
	if err != nil {
		t.Errorf("Open failed: %v", err)
		return
	}

	db.KeepAlive()
	testCreateTable(t, db)
	return
}

func TestNew(t *testing.T) {
	var err error
	sqlx_db, err := sqlx.Open("mysql", "tcp(localhost:3306)")
	if err != nil {
		panic(err)
	}

	_, err = New(sqlx_db)
	if err != nil {
		// this is expected
		// t.Logf("catch expected err message: %v", err)
	} else {
		t.Errorf("error expected but not received")
		return
	}

	// ---
	sqlx_db, err = sqlx.Open("mysql", "tcp(localhost:3306)/db_test")
	if err != nil {
		panic(err)
	}

	db, err := New(sqlx_db)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	} else {
		t.Logf("database: %s", db.Database())
	}
}

func TestMiscError(t *testing.T) {
	var err error
	_, err = Open(Param{
		Port:   1,
		DBName: "db_test",
	})
	if err == nil {
		t.Errorf("error expected error not given")
		return
	}
}
