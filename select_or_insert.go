package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// ========

// SelectOrInsert executes update-if-not-exists statement
func (d *DB) SelectOrInsert(insert interface{}, selectResult interface{}, conds ...interface{}) (res sql.Result, err error) {
	if nil == d.db {
		return nil, fmt.Errorf("mysqlx not initialized")
	}

	// Should be Xxx or *Xxx
	ty := reflect.TypeOf(insert)
	va := reflect.ValueOf(insert)
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Struct != ty.Kind() {
		ty = ty.Elem()
		insert = va.Elem().Interface()
		// log.Printf("%v - %v\n", ty, ty.Kind())
	}

	// Should be Xxx

	// handle select conditions
	parsedArgs, err := d.handleArgs(insert, conds)
	if err != nil {
		// log.Printf("handleArgs() failed: %v", err)
		return nil, err
	}
	if 0 == len(parsedArgs.CondList) {
		return nil, fmt.Errorf("select conditions not given")
	}

	// should have increment field
	incrField, err := d.getIncrementField(insert)
	if err != nil {
		return
	}

	// handle insert fields and values
	keys, values, err := d.InsertFields(insert, false)
	if err != nil {
		return nil, err
	}
	if 0 == len(keys) {
		return nil, fmt.Errorf("no valid values given")
	}

	// log.Printf("keys: %v", keys)
	// log.Printf("values: %v", values)
	var firstList []string
	var secondList []string
	var thirdList []string

	firstList = make([]string, len(keys))
	secondList = make([]string, len(keys))
	thirdList = parsedArgs.CondList

	for i, k := range keys {
		v := values[i]
		firstList[i] = "`" + k + "`"
		secondList[i] = fmt.Sprintf("%s AS %s", v, addQuoteToString(k, "'"))
	}

	var randomField string
	for k := range parsedArgs.FieldMap {
		randomField = k
		break
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) SELECT * FROM (SELECT %s) AS tmp WHERE NOT EXISTS (SELECT `%s` FROM `%s` WHERE %s) LIMIT 1",
		parsedArgs.Opt.TableName, strings.Join(firstList, ", "), strings.Join(secondList, ", "), randomField, parsedArgs.Opt.TableName, strings.Join(thirdList, " AND "),
	)
	// log.Println(query)

	// check table
	err = d.checkAutoCreateTable(insert, parsedArgs.Opt)
	if err != nil {
		return nil, err
	}

	// exec first
	res, err = d.db.Exec(query)
	if err != nil {
		return nil, newError(err.Error(), query)
	}

	if nil == selectResult {
		// simply return
		return res, nil
	}

	// determine insert status
	selectFields, err := d.SelectFields(insert)
	if err != nil {
		return res, err
	}
	insertID, err := res.LastInsertId()
	if err != nil {
		// inserted, now select
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` = %d", selectFields, parsedArgs.Opt.TableName, incrField.Name, insertID)

	} else {
		// not inserted, just select as above
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", selectFields, parsedArgs.Opt.TableName, strings.Join(parsedArgs.CondList, " AND "))
	}

	// log.Println(query)
	return res, d.db.Select(selectResult, query)
}

// ========

//InsertIfNotExists is the same as SelectOrInsert but lacking select statement
func (d *DB) InsertIfNotExists(insert interface{}, conds ...interface{}) (res sql.Result, err error) {
	return d.SelectOrInsert(insert, nil, conds...)
}
