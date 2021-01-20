package mysqlx

import (
	"testing"
)

type department struct {
	ID   int64  `db:"f_id"          mysqlx:"increment:true"`
	Dept string `db:"f_dept"        mysqlx:"type:varchar(128)"`
	Desc string `db:"f_desc"        mysqlx:"type:varchar(256)"`
}

func (department) Options() Options {
	return Options{
		TableName: "t_department",
		Uniques: []Unique{
			Unique{
				Name:   "u_dept",
				Fields: []string{"f_dept"},
			},
		},
		CreateTableParams: map[string]string{
			"AUTO_INCREMENT": "10", // make f_id greater than 1
		},
	}
}

func TestInsertOnDuplicateKeyUpdate(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Errorf("getDB error: %v", err)
		return
	}

	t.Logf("now exec InsertOnDuplicateUpdate")
	db.Sqlx().Exec("DROP TABLE `t_department`")
	db.AutoCreateTable()

	// insert one
	dept := department{
		Dept: "R&D",
		Desc: "IT developers",
	}
	res, err := db.Insert(dept)
	if err != nil {
		t.Errorf("insert error: %v", err)
		return
	}
	insertedID, _ := res.LastInsertId()

	// insert on duplicate update
	dept.Desc = "IT department"

	_, err = db.InsertOnDuplicateKeyUpdate(
		&dept,
		map[string]interface{}{
			"f_desc": dept.Desc,
		},
		Options{DoNotExec: true},
	)
	t.Logf("statement: %v", GetQueryFromError(err))

	res, err = db.InsertOnDuplicateKeyUpdate(
		&dept,
		map[string]interface{}{
			"f_desc": dept.Desc,
		},
	)
	if err != nil {
		t.Errorf("InsertOnDuplicateUpdate error: %v", err)
		return
	}

	showResult(t, res)
	affectedID, _ := res.LastInsertId()
	if insertedID != affectedID {
		t.Errorf("updated id not equal: %d <> %d", insertedID, affectedID)
		return
	}

	// read and check
	var depts []*department
	err = db.Select(&depts)
	if err != nil {
		t.Errorf("Select error: %v", err)
		return
	}

	if 1 != len(depts) {
		t.Errorf("Got records error, len is not 1 (%d)", len(depts))
		return
	}
	if depts[0].Desc != dept.Desc {
		t.Errorf("InsertOnDuplicateUpdate not acted as expected, got string '%s'", depts[0].Desc)
		return
	}
	t.Logf("reord updated as '%s'", depts[0].Desc)

	// test raw statement
	_, err = db.InsertOnDuplicateKeyUpdate(
		&dept,
		map[string]interface{}{
			"f_id": RawStatement("`f_id`"),
		},
	)
	if err != nil {
		t.Errorf("InsertOnDuplicateKeyUpdate with raw statement error: %v", err)
		return
	}

	return
}

func TestInsertManyOnDuplicateKeyUpdate(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Errorf("getDB error: %v", err)
		return
	}

	t.Logf("now exec InsertManyOnDuplicateUpdate")
	db.AutoCreateTable()

	// insert multiple records
	ins := []*department{
		{
			Dept: "R&D",
			Desc: "IT developers", // prev value: 'IT development'
		}, {
			Dept: "Sales",
			Desc: "Sales staff",
		},
	}
	_, err = db.InsertManyOnDuplicateKeyUpdate(
		ins, map[string]interface{}{
			"f_id": Raw("= f_id"),
		},
		Options{DoNotExec: true},
	)
	t.Logf("InsertManyOnDuplicateKeyUpdate statement: %v", GetQueryFromError(err))

	_, err = db.InsertManyOnDuplicateKeyUpdate(
		ins, map[string]interface{}{
			"f_id": Raw("= VALUES(`f_id`)"),
		},
	)
	if err != nil {
		t.Errorf("db.InsertManyOnDuplicateKeyUpdate error: %v", err)
		return
	}

	// check data is not changed
	var res []*department
	err = db.Select(&res)
	if err != nil {
		t.Errorf("db.Select() error: %v", err)
		return
	}
	for _, d := range res {
		if d.Dept == "R&D" {
			if d.Desc == ins[0].Desc {
				t.Errorf("desc is NOT expected to change but changed!")
				return
			}
		}
	}

	return
}
