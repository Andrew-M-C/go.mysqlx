package mysqlx

import (
	"database/sql"
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
		Indexes: []Index{Index{
			Name:   "index_uint2_desu",
			Fields: []string{"uint2"},
		}, Index{
			// Name:   "index_uint",
			Fields: []string{"uint", "uint2"},
		}},
		Uniques: []Unique{Unique{
			Fields: []string{"uint2"},
		}, Unique{
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
	d.MustCreateTable(FirstTable{})
	d.MustCreateTable(SecondTable{}, Options{
		Indexes: []Index{Index{
			Fields: []string{"varchar"},
		}},
	})

	d.MustCreateTable(SecondTable{}, Options{
		TableName:      "t_testB",
		TableDescption: "another table for t_test",
		Uniques: []Unique{Unique{
			Fields: []string{"uint", "uint2"},
		}},
	})
	err := d.CreateTable(struct{}{})
	if err == nil {
		t.Errorf("expected error not raised")
		return
	}
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
	check_expected_error := func(msg string) {
		if err == nil {
			t.Errorf("expected error when '%s' but no error raised", msg)
		} else {
			t.Logf("expected error when '%s': %v", msg, err)
		}
	}

	// ----
	err = d.CreateTable(SimpleStruct{})
	check_expected_error("missing table name")

	// ----
	err = d.CreateTable(SimpleStruct{}, Options{
		TableName: "t_simple",
		Indexes: []Index{
			Index{},
		},
	})
	check_expected_error("missing index content")

	// ----
	err = d.CreateTable(SimpleStruct{}, Options{
		TableName: "t_simple",
		Uniques: []Unique{
			Unique{},
		},
	})
	check_expected_error("missing unique content")

	return
}

type SimpleStruct struct {
	ID   int32  `db:"id"   mysqlx:"increment:true"`
	UUID string `db:"uuid" mysqlx:"type:varchar(32)"`
}
