package mysqlx

import (
	"testing"
)

func TestInsertMany(t *testing.T) {
	db, err := getDB()
	if err != nil {
		t.Errorf("getDB error: %v", err)
		return
	}

	records := []*VarTable{
		&VarTable{
			String: "Hello Asia",
			index:  1,
		},
		&VarTable{
			String: "Hello Europe",
		},
		&VarTable{
			String: "Hello Africa",
		},
	}
	_, err = db.InsertMany(&records, Options{DoNotExec: true})
	t.Logf("statement: %v", GetQueryFromError(err))
	res, err := db.InsertMany(records)
	if err != nil {
		t.Errorf("InsertMany error: %v", err)
		return
	}

	showResult(t, res)

	return
}
