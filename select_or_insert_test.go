package mysqlx

import (
	"database/sql"
	"testing"
	"time"
)

func TestSelectOrInsert(t *testing.T) {
	var err error

	d, err := getDB()
	if err != nil {
		t.Errorf("open failed: %v", err)
		return
	}

	d.Sqlx().Exec("DROP TABLE `t_user`")
	d.AutoCreateTable()

	fields, _ := d.SelectFields(User{})
	t.Logf("User fields: %v", fields)

	abigai := User{
		FirstName:  sql.NullString{Valid: true, String: "Abigail"},
		MiddleName: sql.NullString{Valid: true, String: "E."},
		FamilyName: sql.NullString{Valid: true, String: "Disney"},
		FullName:   "Abigail Disney",
		Gender:     "Female",
		BirthDate:  time.Date(1960, 1, 24, 0, 0, 0, 0, time.UTC),
	}
	var all []User

	// This should be first
	res, err := d.SelectOrInsert(abigai, &all,
		Condition("first_name", "=", "Abigail"),
		Condition("family_name", "=", "Disney"),
	)
	if err != nil {
		t.Errorf("SelectOrInsertOne failed: %v", err)
		return
	}

	showResult(t, res)
	t.Logf("Got return: %+v", all)

	if nil == all || 0 == len(all) {
		t.Errorf("no data returned")
		return
	}

	lastInsertID := all[0].ID

	// second insert
	res, err = d.SelectOrInsert(abigai, &all,
		Condition("first_name", "=", "Abigail"),
		Condition("family_name", "=", "Disney"),
	)

	if err != nil {
		t.Errorf("SelectOrInsertOne failed: %v", err)
		return
	}
	showResult(t, res)
	t.Logf("Got return: %+v", all)

	if nil == all || 0 == len(all) {
		t.Errorf("no data returned")
		return
	}

	if all[0].ID != lastInsertID {
		t.Errorf("duplicated insert detected: %d <> %d", lastInsertID, all[0].ID)
		return
	}

	// simple insert or not exist
	res, err = d.InsertIfNotExists(
		abigai,
		Condition("first_name", "=", "Abigail"),
		Condition("family_name", "=", "Disney"),
	)
	if err != nil {
		t.Errorf("InsertIfNotExists failed")
	} else {
		showResult(t, res)
		insertID, err := res.LastInsertId()
		if err == nil && insertID != 0 {
			t.Errorf("should NOT inserted, got insertID: %d", insertID)
			return
		}
	}

	return
}
