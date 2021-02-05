package mysqlx

import (
	"errors"
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

func testSpecialString(t *testing.T, db DB, id int32, s string) (err error) {
	t.Logf("test special string: %s for id %d", s, id)

	_, err = db.Update(String{},
		map[string]interface{}{
			"string": s,
		},
		Condition("id", "=", id),
	)
	if err != nil {
		t.Errorf("update error: %v", err)
		return
	}

	var res []*String
	err = db.Select(&res, Condition("id", "=", id))
	if err != nil {
		t.Errorf("Select error: %v", err)
		return
	}
	if len(res) == 0 {
		err = errors.New("cannot find record")
		t.Error(err)
		return
	}

	if res[0].S != s {
		t.Errorf("expected string '%s', but got '%s'", s, res[0].S)
		return errors.New("")
	}

	return nil
}

// reference: https://www.cnblogs.com/amylis_chen/archive/2010/07/16/1778921.html
func TestSpecialCharacters(t *testing.T) {
	printf := t.Logf
	errorf := t.Errorf

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		panic(err)
	}

	// statememts, err := db.CreateOrAlterTableStatements(String{})

	db.MustCreateTable(String{})

	// insert
	s := String{
		S:       "initial",
		Created: time.Now().Unix(),
	}
	res, err := db.Insert(s)
	if err != nil {
		errorf("%v", err)
		return
	}
	id, err := res.LastInsertId()
	if err != nil {
		errorf("none inserted %v", err)
		return
	}
	printf("inserted id: %d", id)

	speStrings := []string{
		`<%_％＿'"` + "`" + `%\r\n\t\b	>` + "\r\n\\'\032",
		":::",
		":",
		"::",
		"_",
		"$",
		"%",
		"👈",
		"`",
		"'",
		`"`,
		"\000",
		" ",
		"\t",
		`	`,
		"\\%",
	}

	for _, s := range speStrings {
		err := testSpecialString(t, db, int32(id), s)
		if err != nil {
			return
		}
	}

	// delete it
	// db.Delete(String{}, Condition("id", "=", id))

	return
}
