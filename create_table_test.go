package mysqlx

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
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

	// check if the create table including ROW_FORMAT=DYNAMIC
	_, statements, err := db.CreateOrAlterTableStatements(*line)
	if err != nil {
		errorf("got create table statement error: %v", err)
		return
	}

	stm := statements[0]
	printf("statement: %v", stm)
	if false == strings.Contains(stm, "ROW_FORMAT=DYNAMIC") {
		errorf("not including 'ROW_FORMAT=DYNAMIC'")
		return
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
	err = db.Select(&arr, line.Options(), Options{DoNotExec: true})
	t.Logf("statement: %v", GetQueryFromError(err))
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
	_, err = db.Delete(line, Condition("id", "=", id), Options{DoNotExec: true})
	t.Logf("statement: %v", GetQueryFromError(err))
	err = db.Select(&arr, line.Options())
	res, err = db.Delete(
		line,
		Condition("id", "=", id),
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

	// TableWithTimestamp
	testTableWithTimestamp(t, db)

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
		CreateTableParams: map[string]string{
			"ROW_FORMAT": "DYNAMIC",
		},
	}
}

type TableWithTimestamp struct {
	ID         int32          `db:"id"              mysqlx:"increment:true"`
	Key        string         `db:"key"             mysqlx:"type:varchar(256)"`
	Value      string         `db:"value"           mysqlx:"type:varchar(1024)"`
	CreateTime time.Time      `db:"create_time"     mysqlx:"type:datetime"`
	UpdateTime mysql.NullTime `db:"update_time"     mysqlx:"type:datetime onupdate:CURRENT_TIMESTAMP"`
	DeleteTime sql.NullTime   `db:"delete_time"     mysqlx:"type:datetime"`
}

func (s *TableWithTimestamp) Options() Options {
	if s == nil {
		panic("nil object")
	}
	return Options{
		TableName: "t_with_timestamp",
	}
}

func testTableWithTimestamp(t *testing.T, db *DB) {
	db.Sqlx().Exec("DROP TABLE `t_with_timestamp`")

	createTime := time.Now().UTC()
	s := TableWithTimestamp{
		Key:        "someKey",
		Value:      "someValue",
		CreateTime: createTime,
		UpdateTime: mysql.NullTime{Valid: true, Time: createTime},
		DeleteTime: sql.NullTime{Valid: false},
	}
	res, err := db.Insert(&s)
	if err != nil {
		t.Errorf("db.Insert failed: %v", err)
		return
	}

	insertedID, _ := res.LastInsertId()
	time.Sleep(time.Second)

	_, err = db.Update(&s,
		map[string]interface{}{
			"value": "newSomeValue",
		},
		Condition("id", "=", insertedID),
	)
	if err != nil {
		t.Errorf("db.Update error: %v", err)
		return
	}

	var records []*TableWithTimestamp
	err = db.Select(&records,
		Condition("id", "=", insertedID),
	)
	if err != nil {
		t.Errorf("db.Select error: %v", err)
		return
	}
	if 0 == len(records) {
		t.Errorf("no records selected")
		return
	}

	r := records[0]
	t.Logf("expected tm: %v", s.CreateTime)
	t.Logf("create time: %v", r.CreateTime)
	t.Logf("update Time: %v", r.UpdateTime)

	if r.UpdateTime.Time.Sub(r.CreateTime) >= time.Second {
		t.Logf("check update time OK")
	} else {
		t.Errorf("unexpected update time")
		return
	}

	return
}
