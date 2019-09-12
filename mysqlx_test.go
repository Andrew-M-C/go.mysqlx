package mysqlx

import (
	"testing"

	"github.com/jmoiron/sqlx"
)

func TestOpen(t *testing.T) {
	// successfully open
	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		t.Errorf("Open failed: %v", err)
		return
	}

	d.KeepAlive()
	defer d.StopKeepAlive()

	testCreateTable(t, d)
	testCreateNoAutoIncrement(t, d)
	testCreateTableMiscError(t, d)

	return
}

func TestNew(t *testing.T) {
	var err error
	sqlx_db, err := sqlx.Open("mysql", "travis@tcp(localhost:3306)")
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
	sqlx_db, err = sqlx.Open("mysql", "travis@tcp(localhost:3306)/db_test")
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
		User:   "nouser",
		DBName: "db_test",
	})
	if err == nil {
		t.Errorf("error expected error not given")
		return
	}
}
