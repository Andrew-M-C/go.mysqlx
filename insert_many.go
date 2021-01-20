package mysqlx

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

// InsertMany insert multiple records into table. If additional option with table name is not given,
// mysqlx will use the FIRST table name in records for all.
func (d *xdb) InsertMany(records interface{}, opts ...Options) (result sql.Result, err error) {
	return d.insertMany(d.db, records, opts...)
}

func (d *xdb) insertMany(obj sqlObj, records interface{}, opts ...Options) (result sql.Result, err error) {
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
	log.Printf("%d record(s) given", total)
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

	keys, values, err := d.InsertFields(v, true)
	if err != nil {
		return
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

		_, values, err = d.InsertFields(v, true)
		if err != nil {
			return
		}
		buffVal.WriteString(",\n")
		writeValueToBuff(&buffVal, values)
	}

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

func writeValueToBuff(buff *bytes.Buffer, values []string) {
	buff.WriteRune('(')

	for i, v := range values {
		if i > 0 {
			buff.WriteString(", ")
		}
		buff.WriteString(v)
	}

	buff.WriteRune(')')
	return
}
