package mysqlx

import (
	"testing"
)

type String struct {
	ID int32  `db:"id"     mysqlx:"increment:true"`
	S  string `db:"string" mysqlx:"type:varchar(128)"`
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

	speChars := "%_％＿'"

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		panic(err)
		return
	}

	db.MustCreateTable(String{})
	printf("now test special characters: '%s'", speChars)

	// insert
	s := String{S: speChars}
	res, err := db.Insert(s)
	if err != nil {
		errorf("%v", err)
		return
	}
	if res.LastInsertId == 0 {
		errorf("none inserted")
		return
	}
	printf("inserted: %v", res.LastInsertId)

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

	// update
	_, err = db.Update(
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

	// delete
	_, err = db.Delete(s, Cond{"string", "=", speChars + " "})
	if err != nil {
		errorf("%v", err)
		return
	}

	return
}
