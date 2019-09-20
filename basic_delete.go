package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// ========

// Delete executes SQL DELETE statement with given conditions
func (d *DB) Delete(prototype interface{}, args ...interface{}) (sql.Result, error) {
	if nil == d.db {
		return nil, fmt.Errorf("mysqlx not initialized")
	}

	// Should be Xxx or *Xxx
	ty := reflect.TypeOf(prototype)
	va := reflect.ValueOf(prototype)
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Struct != ty.Kind() {
		ty = ty.Elem()
		prototype = va.Elem().Interface()
		// log.Printf("%v - %v\n", ty, ty.Kind())
	}

	// Should be Xxx

	// parse arguments
	parsedArgs, err := d.handleArgs(prototype, args)
	if err != nil {
		return nil, err
	}

	// pack DELETE statements
	var limitStr string
	if parsedArgs.Limit > 0 {
		limitStr = fmt.Sprintf("LIMIT %d", parsedArgs.Limit)
	}

	var orderStr string
	if len(parsedArgs.OrderList) > 0 {
		orderStr = "ORDER BY " + strings.Join(parsedArgs.OrderList, ", ")
	}

	var condStr string
	if len(parsedArgs.CondList) > 0 {
		condStr = "WHERE " + strings.Join(parsedArgs.CondList, " AND ")
	}

	query := fmt.Sprintf(
		"DELETE FROM `%s` %s %s %s",
		parsedArgs.Opt.TableName, condStr, orderStr, limitStr,
	)
	// log.Println(query)

	return d.db.Exec(query)
}
