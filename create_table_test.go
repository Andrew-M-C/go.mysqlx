package mysqlx

import (
	"database/sql"
	"log"
	"testing"
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
	Uint2        uint
}

type SecondTable struct {
	NewFirstLine int32 `db:"second_table_first_line"`
	FirstTable
	AnotherInt sql.NullInt64
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
create database db_test;
grant all privileges on db_test.* to ''@'localhost';
flush privileges;
*/
func testCreateTable(t *testing.T, db *DB) {
	db.MustCreateTable(FirstTable{})
	db.MustCreateTable(SecondTable{}, Options{
		Indexes: []Index{Index{
			Fields: []string{"varchar"},
		}},
	})

	db.MustCreateTable(SecondTable{}, Options{
		TableName: "t_test_3rd",
		Uniques: []Unique{Unique{
			Fields: []string{"uint", "uint2"},
		}},
	})
	err := db.CreateTable(struct{}{})
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
