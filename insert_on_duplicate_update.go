package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
)

// InsertOnDuplicateKeyUpdate executes 'INSERT ... ON DUPLICATE KEY UPDATE ...' statements.
// This function is a combination of Insert and Update, without WHERE conditions.
func (d *DB) InsertOnDuplicateKeyUpdate(
	v interface{}, updates map[string]interface{}, opts ...Options,
) (result sql.Result, err error) {
	if nil == d.db {
		return nil, fmt.Errorf("nil *sqlx.DB")
	}

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

	keys, values, err := d.InsertFields(v, true)
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

	err = d.checkAutoCreateTable(v, opt)
	if err != nil {
		return nil, err
	}

	return d.db.Exec(sql)
}
