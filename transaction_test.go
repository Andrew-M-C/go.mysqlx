package mysqlx

import (
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func checkError(t *testing.T, err error, s string) {
	if err == nil {
		return
	}
	t.Errorf("%v - %s error: %v", time.Now(), s, err)
	time.Sleep(time.Second)
	os.Exit(1)
}

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
	checkError(t, err, "CreateTable")

	insRes, err := d.Insert(&r)
	checkError(t, err, "Insert")
	id, err := insRes.LastInsertId()
	checkError(t, err, "LastInsertId")

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
	checkError(t, err, "Begin tx1")

	defer func() {
		t.Logf("%v - done for tx1", time.Now())
		err = tx.Commit()
		checkError(t, err, "tx1 Commit")
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
		os.Exit(1)
	}

	err = tx.Select(&resList,
		Condition("f_id", "=", id),
		ForUpdate(), // This will block transaction 2
	)
	checkError(t, err, "tx1 Select")

	// ---- T-03 ----
	waitOneSecond()

	// ---- T-04 ----
	// update
	waitOneSecond()
	_, err = tx.Update(txTestRecord{},
		map[string]interface{}{"f_string": v + "_01"},
		Condition("f_id", "=", id),
	)
	checkError(t, err, "tx1 Update")

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
	checkError(t, err, "Begin tx2")

	defer func() {
		t.Logf("%v - done for tx2", time.Now())
		err = tx.Commit()
		checkError(t, err, "tx2 Commit")
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
	checkError(t, err, "tx2 Select")

	t.Logf("%v - tx2 Got string: %s", time.Now(), resList[0].String)
	if resList[0].String == v {
		t.Errorf("read string value is NOT expected as '%s'!!!", v)
		os.Exit(1)
	}

	// done
	return
}
