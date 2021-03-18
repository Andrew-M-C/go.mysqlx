package mysqlx

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// InsertOnDuplicateKeyUpdate executes 'INSERT ... ON DUPLICATE KEY UPDATE ...' statements.
// This function is a combination of Insert and Update, without WHERE conditions.
func (d *xdb) InsertOnDuplicateKeyUpdate(
	v interface{}, updates map[string]interface{}, opts ...Options,
) (result sql.Result, err error) {
	return d.insertOnDuplicateKeyUpdate(d.db, v, updates, opts...)
}

func (d *xdb) insertOnDuplicateKeyUpdate(
	obj sqlObj, v interface{}, updates map[string]interface{}, opts ...Options,
) (result sql.Result, err error) {

	// Should be *Xxx or Xxx
	ty := reflect.TypeOf(v)
	va := reflect.ValueOf(v)
	prototypeType := ty
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Ptr == ty.Kind() {
		v = va.Elem().Interface()
		ty = reflect.TypeOf(v)
		va = reflect.ValueOf(v)
	}

	// INSERT paramenters
	if reflect.Struct != ty.Kind() {
		return nil, fmt.Errorf("parameter type invalid (%v)", prototypeType)
	}

	keys, values, err := d.insertFields(v, true, false)
	if err != nil {
		return nil, err
	}

	opt := mergeOptions(v, opts...)
	if "" == opt.TableName {
		return nil, fmt.Errorf("empty table name for type %v", reflect.TypeOf(v))
	}

	// UPDATE parameters
	updateKV, err := d.genUpdateKVs(v, updates)
	if err != nil {
		return nil, err
	}
	if 0 == len(updateKV) {
		return nil, fmt.Errorf("no value specified")
	}

	// combine final sql statement
	sql := fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		opt.TableName,
		strings.Join(keys, ", "), strings.Join(values, ", "),
		strings.Join(updateKV, ", "),
	)
	// log.Println(sql)
	if opt.DoNotExec {
		return nil, newError(doNotExec, sql)
	}

	err = d.checkAutoCreateTable(v, opt)
	if err != nil {
		return nil, err
	}

	result, err = obj.Exec(sql)
	if err != nil {
		err = newError(err.Error(), sql)
		return
	}
	return
}

// InsertManyOnDuplicateKeyUpdate is similar with InsertOnDuplicateKeyUpdate, but insert mutiple records for onetime.
func (d *xdb) InsertManyOnDuplicateKeyUpdate(
	records interface{}, updates map[string]interface{}, opts ...Options,
) (result sql.Result, err error) {
	return d.insertManyOnDuplicateKeyUpdate(d.db, records, updates, opts...)
}

func (d *xdb) insertManyOnDuplicateKeyUpdate(
	obj sqlObj, records interface{}, updates map[string]interface{}, opts ...Options,
) (result sql.Result, err error) {

	// records could be *[]*Xxx, []*Xxx, *[]Xxx, []Xxx

	// firstly, get []*Xxx or []Xxx
	va := reflect.ValueOf(records)
	if reflect.Ptr == va.Type().Kind() {
		va = va.Elem()
	}

	// must be []*Xxx or []Xxx
	if reflect.Slice != va.Type().Kind() {
		err = fmt.Errorf("records should be a slice of struct, current type invalid (%v)", va.Type())
		return
	}

	total := va.Len()
	// log.Printf("%d record(s) given", total)
	if 0 == total {
		return nil, errors.New("no records provided")
	}

	// get first element
	isPtr := false
	first := va.Index(0)
	if reflect.Ptr == first.Type().Kind() {
		first = first.Elem()
		isPtr = true
	}
	if reflect.Struct != first.Type().Kind() {
		err = fmt.Errorf("records should be a slice of struct, element type invalid (%v)", first.Type())
		return
	}

	// should be Xxx
	v := first.Interface()

	// get options
	opt := mergeOptions(v, opts...)
	if "" == opt.TableName {
		return nil, fmt.Errorf("empty table name for type %v", reflect.TypeOf(v))
	}

	keys, values, err := d.insertFields(v, true, false)
	if err != nil {
		return
	}

	// UPDATE parameters
	updateKV, err := d.genUpdateKVs(v, updates)
	if err != nil {
		return nil, err
	}
	if 0 == len(updateKV) {
		return nil, fmt.Errorf("no value specified")
	}

	buffVal := bytes.Buffer{}
	buffVal.WriteString(fmt.Sprintf(
		"INSERT INTO `%s` (%s) VALUES\n",
		opt.TableName,
		strings.Join(keys, ", "),
	))

	writeValueToBuff(&buffVal, values)

	// parse other records
	for i := 1; i < total; i++ {
		if isPtr {
			v = va.Index(i).Elem().Interface()
		} else {
			v = va.Index(i).Interface()
		}

		_, values, err = d.insertFields(v, true, true)
		if err != nil {
			return
		}
		buffVal.WriteString(",\n")
		writeValueToBuff(&buffVal, values)
	}

	buffVal.WriteString("\nON DUPLICATE KEY UPDATE\n")

	for i, kv := range updateKV {
		if i > 0 {
			buffVal.WriteString(", ")
		}
		buffVal.WriteString(kv)
	}

	// combine final sql statement
	query := buffVal.String()
	if opt.DoNotExec {
		return nil, newError(doNotExec, query)
	}

	err = d.checkAutoCreateTable(v, opt)
	if err != nil {
		return nil, err
	}
	result, err = obj.Exec(query)
	if err != nil {
		err = newError(err.Error(), query)
		return
	}
	return
}
