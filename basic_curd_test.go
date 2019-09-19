package mysqlx

import (
	"database/sql"
	"testing"
	"time"

	mariadb "github.com/go-sql-driver/mysql"
)

type User struct {
	ID              int32            `db:"id"               mysqlx:"increment:true"`
	FirstName       sql.NullString   `db:"first_name"       mysqlx:"type:varchar(20)"`
	MiddleName      sql.NullString   `db:"middle_name"      mysqlx:"type:varchar(100)"`
	FamilyName      sql.NullString   `db:"family_name"      mysqlx:"type:varchar(20)"`
	FullName        string           `db:"full_name"        mysqlx:"type:varchar(100)"`
	Gender          string           `db:"gender"           mysqlx:"type:char(10)"`
	BirthDate       time.Time        `db:"birth_date"       mysqlx:"type:date"`
	Nationality     string           `db:"nation"           mysqlx:"type:varchar(50)"`
	UpdateTimestamp int64            `db:"update_timestamp"`
	Certified       sql.NullBool     `db:"certified"`
	StatusMasks     uint64           `db:"status_masks"`
	DieTime         mariadb.NullTime `db:"die_time"`
}

type Disney struct {
	User
	IsBoss bool `db:"is_boss"`
}

func (User) Options() Options {
	return Options{
		TableName:      "t_user",
		TableDescption: "general user information",
		Indexes: []Index{
			Index{
				Name:   "index_fullname",
				Fields: []string{"full_name"},
			},
			Index{
				Name:   "index_lastname",
				Fields: []string{"family_name", "first_name"},
			},
		},
	}
}

func TestQuery(t *testing.T) {
	var err error

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		t.Errorf("open failed: %v", err)
		return
	}

	err = db.CreateTable(User{})
	if err != nil {
		t.Errorf("Create User error: %v", err)
		return
	}

	err = db.CreateTable(Disney{})
	if err != nil {
		t.Errorf("Create Disney error: %v", err)
		return
	}

	// const SHOULD_BE = "`id`, `first_name`, `middle_name`, `family_name`, `full_name`, `gender`, `birth_date`, `nation`"
	select_fields, err := db.SelectFields(User{})
	if err != nil {
		return
	}
	t.Logf("fields: %s", select_fields)

	// insert one
	new_disney := Disney{}
	new_disney.FirstName = sql.NullString{Valid: true, String: "Walter"}
	new_disney.MiddleName = sql.NullString{Valid: true, String: "Elias"}
	new_disney.FamilyName = sql.NullString{Valid: true, String: "Disney"}
	new_disney.FullName = "Walter Disney"
	new_disney.Gender = "Male"
	new_disney.BirthDate = time.Date(1901, 12, 5, 0, 0, 0, 0, time.UTC)
	new_disney.Nationality = "U.S."
	new_disney.UpdateTimestamp = time.Now().Unix()
	new_disney.Certified = sql.NullBool{Valid: true, Bool: true}
	new_disney.DieTime = mariadb.NullTime{Valid: true, Time: time.Date(1966, 12, 15, 14, 30, 0, 0, time.UTC)}
	new_disney.IsBoss = true

	keys, values, err := db.InsertFields(new_disney, true)
	if err != nil {
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	res, err := db.Insert(new_disney)
	if err != nil {
		t.Errorf("Insert Walter Disney error: %v", err)
		return
	}
	showResult(t, res)

	// insert another one thrice
	new_user := User{
		FirstName:       sql.NullString{Valid: true, String: "Diane"},
		MiddleName:      sql.NullString{Valid: true, String: "Disney"},
		FamilyName:      sql.NullString{Valid: true, String: "Miller"},
		FullName:        "Diane Miller",
		Gender:          "Female",
		BirthDate:       time.Date(1933, 4, 17, 0, 0, 0, 0, time.UTC),
		Nationality:     "U.S.",
		UpdateTimestamp: time.Now().Unix(),
	}

	keys, values, err = db.InsertFields(new_user, false)
	if err != nil {
		t.Errorf("Insert Diane Miller error: %v", err)
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	// insert struct
	for _ = range make([]int, 3) {
		res, err := db.Insert(new_user)
		if err != nil {
			return
		}
		showResult(t, res)
	}

	// insert pointer
	res, err = db.Insert(new_user)
	if err != nil {
		return
	}
	showResult(t, res)

	// select
	var result []Disney
	err = db.Select(
		&result,
		Cond{"family_name", "<>", "Disney"},
		Cond{"die_time", "=", nil}, // for MySQL NULL, should be "IS" or "IS NOT", but here er make some compatibility
		Cond{"birth_date", ">=", time.Date(1910, 1, 1, 0, 0, 0, 0, time.UTC)},
		Offset{1}, Limit{2},
		Order{"id", "DESC"},
	)
	if err != nil {
		t.Errorf("select disney error: %v", err)
		return
	}
	if nil == result || 0 == len(result) {
		t.Errorf("no selection returned")
	} else {
		t.Logf("Get %d response(s)", len(result))
	}

	// update
	res, err = db.Update(
		Disney{}, map[string]interface{}{
			"die_time": time.Date(2013, 9, 19, 0, 0, 0, 0, time.UTC),
			"is_boss":  true,
		},
		Cond{"first_name", "=", "Diane"},
		Cond{"family_name", "=", "Miller"},
		Limit{1},
	)
	if err != nil {
		t.Errorf("Update failed: %v", err)
		return
	}

	affected, err := res.RowsAffected()
	if err != nil {
		t.Errorf("read RowsAffected error: %v", err)
		return
	}
	t.Logf("affected row(s): %d", affected)

	// delete
	res, err = db.Delete(
		Disney{},
		Cond{"first_name", "=", "Diane"},
		Cond{"family_name", "=", "Miller"},
		Cond{"die_time", "IS", nil},
	)
	if err != nil {
		t.Errorf("Update failed: %v", err)
		return
	}

	affected, err = res.RowsAffected()
	if err != nil {
		t.Errorf("read RowsAffected error: %v", err)
		return
	}
	t.Logf("affected row(s): %d", affected)
	return
}

func TestSelectOrInsert(t *testing.T) {
	var err error

	d, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		t.Errorf("open failed: %v", err)
		return
	}
	d.MustCreateTable(User{})

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
		Cond{"first_name", "=", "Abigail"},
		Cond{"family_name", "=", "Disney"},
	)

	if err != nil {
		t.Errorf("SelectOrInsertOne failed: %v", err)
		return
	} else {
		showResult(t, res)
		t.Logf("Got return: %+v", all)
	}
	if nil == all || 0 == len(all) {
		t.Errorf("no data returned")
		return
	}

	last_insert_id := all[0].ID

	// second insert
	res, err = d.SelectOrInsert(abigai, &all,
		Cond{"first_name", "=", "Abigail"},
		Cond{"family_name", "=", "Disney"},
	)

	if err != nil {
		t.Errorf("SelectOrInsertOne failed: %v", err)
		return
	} else {
		showResult(t, res)
		t.Logf("Got return: %+v", all)
	}
	if nil == all || 0 == len(all) {
		t.Errorf("no data returned")
		return
	}

	if all[0].ID != last_insert_id {
		t.Errorf("duplicated insert detected: %d <> %d", last_insert_id, all[0].ID)
		return
	}

	// simple insert or not exist
	res, err = d.InsertIfNotExists(
		abigai,
		Cond{"first_name", "=", "Abigail"},
		Cond{"family_name", "=", "Disney"},
	)
	if err != nil {
		t.Errorf("InsertIfNotExists failed")
	} else {
		showResult(t, res)
		insert_id, err := res.LastInsertId()
		if err == nil && insert_id != 0 {
			t.Errorf("should NOT inserted, got insert_id: %d", insert_id)
			return
		}
	}

	return
}

func showResult(t *testing.T, res sql.Result) {
	last_insert_id, err := res.LastInsertId()
	if err == nil {
		t.Logf("LastInsertId = %d", last_insert_id)
	}

	affected, err := res.RowsAffected()
	if err == nil {
		t.Logf("RowsAffected = %d", affected)
	}

	return
}
