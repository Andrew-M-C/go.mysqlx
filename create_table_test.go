package mysqlx

import (
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

type FirstTable struct {
	innerPara    string
	ID           int64          `db:"id"              mysqlx:"type:bigint(12) null:false increment:true" comment:"main ID"`
	NullableInt  sql.NullInt64  `db:"nullable_int"    mysqlx:"type:int(11) null:true"                    comment:"test a nullable integer"`
	NotNullInt32 int32          `db:"not_null_int32"  mysqlx:"type:smallint null:false"                  comment:"unullable smallint"`
	VarChar      string         `db:"varchar"         mysqlx:"type:varchar(128) null:false"              comment:"unullable varchar"`
	NullVarChar  sql.NullString `db:"null_varchar"    mysqlx:"type:varchar(64)"                          comment:"nullable varchar"`
	Uint1        uint           `db:"uint"            mysqlx:"type:uint"`
	Uint2        uint           `db:"uint2"`
	Timestamp    time.Time      `db:"test_time"       mysqlx:"type:timestamp"`
}

type SecondTable struct {
	NewFirstLine int32 `db:"second_table_first_line"`
	FirstTable
	AnotherInt sql.NullInt64 `db:"another_int"`
}

func (FirstTable) Options() Options {
	return Options{
		TableName:      "t_test",
		TableDescption: "test table for CreateTable",
		Indexes: []Index{{
			Name:   "index_uint2_desu",
			Fields: []string{"uint2"},
		}, {
			// Name:   "index_uint",
			Fields: []string{"uint", "uint2"},
		}},
		Uniques: []Unique{{
			Fields: []string{"uint2"},
		}, {
			Fields: []string{"uint", "uint2"},
		}},
	}
}

/* pre:
CREATE USER 'travis'@'localhost';
create database db_test;
grant all privileges on db_test.* to 'travis'@'localhost';
flush privileges;
*/
func testCreateTable(t *testing.T, d *DB) {
	d.Sqlx().Exec("DROP TABLE `t_test`")
	d.Sqlx().Exec("DROP TABLE `t_testB`")

	exists, statements, err := d.CreateOrAlterTableStatements(FirstTable{})
	if err != nil {
		t.Errorf("CreateAndAlterTableStatements error: %v", err)
		return
	}
	t.Logf("For FirstTable{}")
	t.Logf("exists: %v", exists)
	t.Logf("statements: %v", statements)

	d.MustCreateTable(FirstTable{})

	secondOpt := Options{
		Indexes: []Index{{
			Fields: []string{"varchar"},
		}},
	}
	exists, statements, err = d.CreateOrAlterTableStatements(SecondTable{}, secondOpt)
	if err != nil {
		t.Errorf("CreateAndAlterTableStatements error: %v", err)
		return
	}
	t.Logf("For SecondTable{}")
	t.Logf("exists: %v", exists)
	t.Logf("statements: %v", statements)

	d.MustCreateTable(SecondTable{}, secondOpt)

	d.MustCreateTable(SecondTable{}, Options{
		TableName:      "t_testB",
		TableDescption: "another table for t_test",
		Uniques: []Unique{{
			Fields: []string{"uint", "uint2"},
		}},
	})
	err = d.CreateTable(struct{}{})
	if err == nil {
		t.Errorf("expected error not raised")
		return
	}

	indexes, uniques, err := d.ReadTableIndexes("t_test")
	if err != nil {
		t.Errorf("d.ReadTableIndexes error: %v", err)
	}
	t.Logf("indexes: %+v", indexes)
	t.Logf("uniques: %+v", uniques)
	return
}

func TestPanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("error expected but not catched")
		}
	}()

	d := &DB{}
	d.MustCreateTable(FirstTable{})

	return
}

func testCreateNoAutoIncrement(t *testing.T, d *DB) {
	d.Sqlx().Exec("DROP TABLE `t_no_inc`")
	d.MustCreateTable(
		struct {
			Count int64 `db:"count"`
		}{},
		Options{TableName: "t_no_inc"},
	)
}

// some expection / error test
func testCreateTableMiscError(t *testing.T, d *DB) {
	var err error
	checkExpectedError := func(msg string) {
		if err == nil {
			t.Errorf("expected error when '%s' but no error raised", msg)
		} else {
			t.Logf("expected error when '%s': %v", msg, err)
		}
	}

	// ----
	err = d.CreateTable(SimpleStruct{})
	checkExpectedError("missing table name")

	// ----
	err = d.CreateTable(SimpleStruct{}, Options{
		TableName: "t_simple",
		Indexes: []Index{
			{},
		},
	})
	checkExpectedError("missing index content")

	// ----
	err = d.CreateTable(SimpleStruct{}, Options{
		TableName: "t_simple",
		Uniques: []Unique{
			{},
		},
	})
	checkExpectedError("missing unique content")

	return
}

type SimpleStruct struct {
	ID   int32  `db:"id"   mysqlx:"increment:true"`
	UUID string `db:"uuid" mysqlx:"type:varchar(32)"`
}

// test pointer struct
func TestVariousStruct(t *testing.T) {
	errorf := t.Errorf
	printf := t.Logf

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		t.Errorf("open failed: %v", err)
		return
	}

	db.AutoCreateTable()
	db.Sqlx().Exec("DROP TABLE `t_vartable_00`")
	db.Sqlx().Exec("DROP TABLE `t_vartable_01`")
	db.Sqlx().Exec("DROP TABLE `t_vartable_02`")

	line := &VarTable{
		String: "Hello, mysqlx!",
		index:  1,
	}

	// insert
	res, err := db.Insert(line)
	if err != nil {
		errorf("db.Insert() error: %v", err)
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		errorf("res.LastInsertId() error: %v", err)
		return
	}
	printf("inserted %d", id)

	// update
	res, err = db.Update(
		line,
		map[string]interface{}{
			"str": "Hello, MySQLx!",
		},
	)
	if err != nil {
		errorf("Update error: %v", err)
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		errorf("RowsAffected error: %v", err)
		return
	}
	if affected == 0 {
		errorf("none affected")
		return
	}

	// select
	var arr []*VarTable
	err = db.Select(&arr, line.Options())
	if err != nil {
		errorf("Select() error: %v", err)
		return
	}
	if 0 == len(arr) {
		errorf("none selected")
		return
	}

	if arr[0].String == line.String {
		errorf("selected string '%s' should NOT equal", arr[0].String)
		return
	}

	// delete
	res, err = db.Delete(
		line,
		Cond{"id", "=", id},
	)
	if err != nil {
		errorf("Delete error: %v", err)
		return
	}
	affected, err = res.RowsAffected()
	if err != nil {
		errorf("none affected? %v", err)
		return
	}
	if 0 == affected {
		errorf("none affected")
		return
	}

	return
}

type VarTable struct {
	ID     int32  `db:"id"   mysqlx:"increment:true"`
	String string `db:"str"  mysqlx:"type:varchar(256)"`
	index  int
}

func (s *VarTable) Options() Options {
	if nil == s {
		panic("nil object")
	}
	tableName := fmt.Sprintf("t_vartable_%02d", s.index)
	log.Printf("table name: '%s'", tableName)

	return Options{
		TableName: tableName,
	}
}
