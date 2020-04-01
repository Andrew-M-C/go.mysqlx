package mysqlx

import (
	"testing"
	"time"
)

type String struct {
	ID      int32  `db:"id"               mysqlx:"increment:true"`
	S       string `db:"string"           mysqlx:"type:varchar(128)"`
	Created int64  `db:"create_timestamp"`
}

func (String) Options() Options {
	return Options{
		TableName: "t_string",
	}
}

// reference: https://www.cnblogs.com/amylis_chen/archive/2010/07/16/1778921.html
func TestSpecialCharacters(t *testing.T) {
	printf := t.Logf
	errorf := t.Errorf

	speChars := `<%_％＿'"` + "`" + `%\r\n\t\b	>` + "\r\n\\'\032"

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		panic(err)
		return
	}

	// statememts, err := db.CreateOrAlterTableStatements(String{})

	db.MustCreateTable(String{})
	printf("test spe string: '%s'", speChars)

	// insert
	s := String{
		S:       speChars,
		Created: time.Now().Unix(),
	}
	res, err := db.Insert(s)
	if err != nil {
		errorf("%v", err)
		return
	}
	last, err := res.LastInsertId()
	if err != nil {
		errorf("none inserted %v", err)
		return
	}
	printf("inserted id: %d", last)

	// select
	var arr []String
	err = db.Select(
		&arr,
		Cond{"string", "=", speChars},
	)
	if err != nil {
		errorf("%v", err)
		return
	}
	if 0 == len(arr) {
		errorf("nothing got")
		return
	}
	printf("got spe string:  '%s'", arr[0].S)
	if arr[0].S != speChars {
		errorf("expected <%s>, got <%s>", speChars, arr[0].S)
		return
	}

	// update
	res, err = db.Update(
		s,
		map[string]interface{}{
			"string": speChars + " ",
		},
		Cond{"string", "=", speChars},
	)
	if err != nil {
		errorf("%v", err)
		return
	}
	if affected, _ := res.RowsAffected(); 0 == affected {
		errorf("none affected")
		return
	}

	// delete
	res, err = db.Delete(s, Cond{"string", "=", speChars + " "})
	if err != nil {
		errorf("%v", err)
		return
	}
	affected, err := res.RowsAffected()
	if err != nil {
		errorf("affected err: %v", err)
		return
	}
	if 0 == affected {
		errorf("none affected")
		return
	}

	return
}
