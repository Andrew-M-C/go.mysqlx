package mysqlx

import (
	"strings"
	"sync"
	"testing"
	"time"
)

type txTestRecord struct {
	ID     int64  `db:"f_id"           mysqlx:"increment:true"`
	String string `db:"f_string"       mysqlx:"type:varchar(128)"`
}

func (r *txTestRecord) Options() Options {
	return Options{
		TableName: "t_mysqlx_tx_test",
	}
}

func TestTransaction(t *testing.T) {
	d, err := getDB()
	if err != nil {
		t.Errorf("open failed: %v", err)
		return
	}

	// fristly, create an record to operate
	v := "transaction"
	r := txTestRecord{String: v}
	err = d.CreateTable(&r)
	if err != nil {
		t.Errorf("CreateTable error: %v", err)
		return
	}

	insRes, err := d.Insert(&r)
	if err != nil {
		t.Errorf("Insert: %v", err)
		return
	}
	id, err := insRes.LastInsertId()
	if err != nil {
		t.Errorf("LastInsertId error: %v", err)
		return
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go testTx01(t, d, id, v, &wg)
	wg.Add(1)
	go testTx02(t, d, id, v, &wg)

	wg.Wait()
	t.Logf("transaction test done")
	return
}

func waitOneSecond() {
	time.Sleep(time.Second)
}

func testTx01(t *testing.T, d DB, id int64, v string, wg *sync.WaitGroup) {
	defer wg.Done()

	// ---- T-00 ----
	// start first transaction
	waitOneSecond()
	t.Logf("%v - Begin tx1", time.Now())
	tx, err := d.Begin()
	if err != nil {
		t.Errorf("Begin tx1 error: %v", err)
		return
	}

	defer func() {
		t.Logf("%v - done for tx1", time.Now())
		err = tx.Commit()
		if err != nil {
			t.Errorf("tx1 Commit error: %v", err)
		}
	}()

	// ---- T-01 ----
	waitOneSecond()

	// ---- T-02 ----
	// select for update
	waitOneSecond()
	t.Logf("%v - tx1 select for record", time.Now())
	var resList []*txTestRecord
	err = tx.Select(&resList,
		Condition("f_id", "=", id),
		ForUpdate(),
		Options{DoNotExec: true},
	)
	sql := GetQueryFromError(err)
	t.Logf("%v - Got SQL: %s", time.Now(), sql)
	if !strings.Contains(sql, "FOR UPDATE") {
		t.Errorf("sql do not contains FOR UPDATE!")
		return
	}

	err = tx.Select(&resList,
		Condition("f_id", "=", id),
		ForUpdate(), // This will block transaction 2
	)
	if err != nil {
		t.Errorf("tx1 Select error: %v", err)
		return
	}

	// ---- T-03 ----
	waitOneSecond()

	// ---- T-04 ----
	// update
	waitOneSecond()
	_, err = tx.Update(txTestRecord{},
		map[string]interface{}{"f_string": v + "_01"},
		Condition("f_id", "=", id),
	)
	if err != nil {
		t.Errorf("tx1 Update error: %v", err)
		return
	}

	// done
	return
}

func testTx02(t *testing.T, d DB, id int64, v string, wg *sync.WaitGroup) {
	defer wg.Done()

	// ---- T-00 ----
	waitOneSecond()

	// ---- T-01 ----
	waitOneSecond()
	t.Logf("%v - Begin tx2", time.Now())
	tx, err := d.Begin()
	if err != nil {
		t.Errorf("Begin tx2 error: %v", err)
		return
	}

	defer func() {
		t.Logf("%v - done for tx2", time.Now())
		err = tx.Commit()
		if err != nil {
			t.Errorf("tx2 Commit error: %v", err)
		}
	}()

	// ---- T-02 ----
	waitOneSecond()

	// ---- T-03 ----
	// this SELECT should be blocked!
	waitOneSecond()
	var resList []*txTestRecord
	err = tx.Select(&resList,
		Condition("f_id", "=", id),
		ForUpdate(),
	)
	if err != nil {
		t.Errorf("tx2 Select error: %v", err)
		return
	}

	t.Logf("%v - tx2 Got string: %s", time.Now(), resList[0].String)
	if resList[0].String == v {
		t.Errorf("read string value is NOT expected as '%s'!!!", v)
		return
	}

	// done
	return
}
