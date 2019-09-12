package mysqlx

import (
	"database/sql"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

type User struct {
	ID              int32          `db:"id"               mysqlx:"increment:true"`
	FirstName       sql.NullString `db:"first_name"       mysqlx:"type:varchar(20)"`
	MiddleName      sql.NullString `db:"middle_name"      mysqlx:"type:varchar(100)"`
	FamilyName      sql.NullString `db:"family_name"      mysqlx:"type:varchar(20)"`
	FullName        string         `db:"full_name"        mysqlx:"type:varchar(100)"`
	Gender          string         `db:"gender"           mysqlx:"type:char(10)"`
	BirthDate       time.Time      `db:"birth_date"       mysqlx:"type:date"`
	Nationality     string         `db:"nation"           mysqlx:"type:varchar(50)"`
	UpdateTimestamp int64          `db:"update_timestamp"`
	Certified       sql.NullBool   `db:"certified"`
	StatusMasks     uint64         `db:"status_masks"`
	DieTime         mysql.NullTime `db:"die_time"`
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

	const SHOULD_BE = "`id`, `first_name`, `middle_name`, `family_name`, `full_name`, `gender`, `birth_date`, `nation`"
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
	new_disney.FullName = "Walter Elias Disney"
	new_disney.Gender = "Male"
	new_disney.BirthDate = time.Date(1901, 12, 5, 0, 0, 0, 0, time.UTC)
	new_disney.Nationality = "U.S."
	new_disney.UpdateTimestamp = time.Now().Unix()
	new_disney.Certified = sql.NullBool{Valid: true, Bool: true}
	new_disney.DieTime = mysql.NullTime{Valid: true, Time: time.Date(1966, 12, 15, 14, 30, 0, 0, time.UTC)}
	new_disney.IsBoss = true

	keys, values, err := db.InsertFields(new_disney)
	if err != nil {
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	id, err := db.Insert(new_disney)
	if err != nil {
		t.Errorf("Insert Walter Disney error: %v", err)
		return
	}

	t.Logf("inserted id: %d", id)

	// insert another one
	new_user := User{
		FirstName:       sql.NullString{Valid: true, String: "Diane"},
		MiddleName:      sql.NullString{Valid: true, String: "Disney"},
		FamilyName:      sql.NullString{Valid: true, String: "Miller"},
		FullName:        "Diane Disney Miller",
		Gender:          "Female",
		BirthDate:       time.Date(1933, 4, 17, 0, 0, 0, 0, time.UTC),
		Nationality:     "U.S.",
		UpdateTimestamp: time.Now().Unix(),
	}

	keys, values, err = db.InsertFields(new_user)
	if err != nil {
		t.Errorf("Insert Diane Miller error: %v", err)
		return
	}
	t.Logf("Keys: %v", keys)
	t.Logf("Vals: %v", values)

	id, err = db.Insert(new_user)
	if err != nil {
		return
	}

	t.Logf("inserted id: %d", id)
	return
}
