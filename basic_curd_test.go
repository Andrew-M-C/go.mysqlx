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
	IgnoreField     time.Time        `db:"-"`
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

func getDB() (*DB, error) {
	return Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
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

	// read back
	var result []*Disney
	lastID, _ := res.LastInsertId()
	err = db.Select(&result, Condition("id", "=", lastID))
	if err != nil {
		t.Errorf("db.Select error: %v", err)
		return
	}
	if result[0].FirstName != newDisney.FirstName {
		t.Errorf("unexpected result: %+v", result[0])
		return
	}
	t.Logf("Got id=%d result: %+v", lastID, result[0])

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
	err = db.Select(
		&result,
		Condition("family_name", "<>", "Disney"),
		Condition("die_time", "=", nil), // for MySQL NULL, should be "IS" or "IS NOT", but here er make some compatibility
		Condition("birth_date", ">=", time.Date(1910, 1, 1, 0, 0, 0, 0, time.UTC)),
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

	// select with OR
	result = nil
	err = db.Select(
		&result,
		Or{
			Condition("first_name", "=", "Diane"),
			Condition("first_name", "=", "Walter"),
		},
	)
	if err != nil {
		t.Errorf("select all disney error: %v", err)
		return
	}
	if nil == result || 0 == len(result) {
		t.Errorf("no OR selection returned")
	} else {
		t.Logf("Get OR %d response(s)", len(result))
	}
	for _, u := range result {
		t.Logf("%+v", u)
	}

	// update
	res, err = db.Update(
		Disney{}, map[string]interface{}{
			"die_time": time.Date(2013, 9, 19, 0, 0, 0, 0, time.UTC),
			"is_boss":  true,
		},
		Condition("first_name", "=", "Diane"),
		Condition("family_name", "=", "Miller"),
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

	// select with in
	result = nil
	err = db.Select(
		&result,
		And{
			Condition("first_name", "in", []string{"Diane", "Walter"}),
			Condition("update_timestamp", "!=", 0),
		},
	)
	if err != nil {
		t.Errorf("select with IN failed: %v", err)
		return
	}
	if len(result) < 2 {
		t.Errorf("unexpected result count %d", len(result))
		return
	}

	err = db.Select(
		&result,
		Condition("update_timestamp", "in", []int32{1, 2, 3}),
	)
	if err != nil {
		t.Errorf("select with IN failed: %v", err)
		return
	}

	// delete
	res, err = db.Delete(
		Disney{},
		Condition("first_name", "=", "Diane"),
		Condition("family_name", "=", "Miller"),
		Condition("die_time", "IS", nil),
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
