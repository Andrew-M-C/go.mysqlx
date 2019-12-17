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
			{
				Name:   "index_fullname",
				Fields: []string{"full_name"},
			},
			{
				Name:   "index_lastname",
				Fields: []string{"family_name", "first_name"},
			},
		},
	}
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

	d.Sqlx().Exec("DROP TABLE `t_user`")
	d.AutoCreateTable()

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
		Cond{"first_name", "=", "Abigail"},
		Cond{"family_name", "=", "Disney"},
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
		Cond{"first_name", "=", "Abigail"},
		Cond{"family_name", "=", "Disney"},
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

func showResult(t *testing.T, res sql.Result) {
	lastInsertID, err := res.LastInsertId()
	if err == nil {
		t.Logf("LastInsertId = %d", lastInsertID)
	}

	affected, err := res.RowsAffected()
	if err == nil {
		t.Logf("RowsAffected = %d", affected)
	}

	return
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
	selectFields, err := db.SelectFields(User{})
	if err != nil {
		return
	}
	t.Logf("fields: %s", selectFields)

	// insert one
	newDisney := Disney{}
	newDisney.FirstName = sql.NullString{Valid: true, String: "Walter"}
	newDisney.MiddleName = sql.NullString{Valid: true, String: "Elias"}
	newDisney.FamilyName = sql.NullString{Valid: true, String: "Disney"}
	newDisney.FullName = "Walter Disney"
	newDisney.Gender = "Male"
	newDisney.BirthDate = time.Date(1901, 12, 5, 0, 0, 0, 0, time.UTC)
	newDisney.Nationality = "U.S."
	newDisney.UpdateTimestamp = time.Now().Unix()
	newDisney.Certified = sql.NullBool{Valid: true, Bool: true}
	newDisney.DieTime = mariadb.NullTime{Valid: true, Time: time.Date(1966, 12, 15, 14, 30, 0, 0, time.UTC)}
	newDisney.IsBoss = true

	keys, values, err := db.InsertFields(newDisney, true)
	if err != nil {
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	res, err := db.Insert(newDisney)
	if err != nil {
		t.Errorf("Insert Walter Disney error: %v", err)
		return
	}
	showResult(t, res)

	// insert another one thrice
	newUser := User{
		FirstName:       sql.NullString{Valid: true, String: "Diane"},
		MiddleName:      sql.NullString{Valid: true, String: "Disney"},
		FamilyName:      sql.NullString{Valid: true, String: "Miller"},
		FullName:        "Diane Miller",
		Gender:          "Female",
		BirthDate:       time.Date(1933, 4, 17, 0, 0, 0, 0, time.UTC),
		Nationality:     "U.S.",
		UpdateTimestamp: time.Now().Unix(),
	}

	keys, values, err = db.InsertFields(newUser, false)
	if err != nil {
		t.Errorf("Insert Diane Miller error: %v", err)
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	// insert struct
	for range make([]int, 3) {
		res, err := db.Insert(newUser)
		if err != nil {
			return
		}
		showResult(t, res)
	}

	// insert pointer
	res, err = db.Insert(newUser)
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
