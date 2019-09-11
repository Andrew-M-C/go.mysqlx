package mysqlx

import (
	"database/sql"
	"testing"
	"time"
)

type User struct {
	ID          int32          `db:"id"`
	FirstName   sql.NullString `db:"first_name"     mysqlx:"type:varchar(20)"`
	MiddleName  sql.NullString `db:"middle_name"    mysqlx:"type:varchar(100)"`
	FamilyName  sql.NullString `db:"family_name"    mysqlx:"type:varchar(20)"`
	FullName    string         `db:"full_name"      mysqlx:"type:varchar(100)"`
	Gender      string         `db:"gender"         mysqlx:"type:char(10)"`
	BirthDate   time.Time      `db:"birth_date"     mysqlx:"type:date"`
	Nationality string         `db:"nation"         mysqlx:"type:varchar(50)"`
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
	defer func() {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	}()

	db, err := Open(Param{
		User:   "travis",
		DBName: "db_test",
	})
	if err != nil {
		return
	}

	err = db.CreateTable(User{})
	if err != nil {
		return
	}

	// insert one
	// new_user := User{
	// 	FirstName:   sql.NullString{Valid: true, String: "Walter"},
	// 	MiddleName:  sql.NullString{Valid: true, String: "Elias"},
	// 	FamilyName:  sql.NullString{Valid: true, String: "Disney"},
	// 	FullName:    "Walter Elias Disney",
	// 	Gender:      "Male",
	// 	BirthDate:   time.Date(1901, 12, 5, 0, 0, 0, 0, nil),
	// 	Nationality: "U.S.",
	// }
}
