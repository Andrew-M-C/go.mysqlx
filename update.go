package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// ========

// Update execute UPDATE SQL statement with given structure and conditions
func (d *xdb) Update(
	prototype interface{}, fields map[string]interface{}, args ...interface{},
) (sql.Result, error) {
	return d.update(d.db, prototype, fields, args...)
}

func (d *xdb) update(
	obj sqlObj, prototype interface{}, fields map[string]interface{}, args ...interface{},
) (sql.Result, error) {
	if nil == fields || 0 == len(fields) {
		return nil, fmt.Errorf("nil fields")
	}

	// Should be *Xxx or Xxx
	ty := reflect.TypeOf(prototype)
	va := reflect.ValueOf(prototype)
	prototypeType := ty
	// debugf("%v - %v\n", ty, ty.Kind())
	if reflect.Ptr == ty.Kind() {
		prototype = va.Elem().Interface()
		ty = reflect.TypeOf(prototype)
		va = reflect.ValueOf(prototype)
	}

	if reflect.Struct != ty.Kind() {
		return nil, fmt.Errorf("parameter type invalid (%v)", prototypeType)
	}

	opt := mergeOptions(prototype)
	var limitStr string
	var condStr string

	// handle fields
	kv, err := d.genUpdateKVs(prototype, fields)
	if err != nil {
		return nil, err
	}
	if 0 == len(kv) {
		return nil, fmt.Errorf("no value specified")
	}

	parsedArgs, err := d.handleArgs(prototype, args)
	if err != nil {
		return nil, err
	}
	if parsedArgs.Limit > 0 {
		limitStr = "LIMIT " + strconv.Itoa(parsedArgs.Limit)
	}

	if len(parsedArgs.CondList) > 0 {
		condStr = "WHERE " + strings.Join(parsedArgs.CondList, " AND ")
	}

	query := fmt.Sprintf("UPDATE `%s` SET %s %s %s", opt.TableName, strings.Join(kv, ", "), condStr, limitStr)
	// log.Println(query)
	if parsedArgs.Opt.DoNotExec {
		return nil, newError(doNotExec, query)
	}

	err = d.checkAutoCreateTable(prototype, parsedArgs.Opt)
	if err != nil {
		return nil, err
	}

	// UPDATE
	res, err := obj.Exec(query)
	if err != nil {
		err = newError(err.Error(), query)
		return nil, err
	}
	return res, err
}

func (d *xdb) genUpdateKVs(prototype interface{}, fields map[string]interface{}) ([]string, error) {
	fieldMap, err := d.getFieldMap(prototype)
	if err != nil {
		return nil, err
	}

	kv := make([]string, 0, len(fields))
	for k, v := range fields {
		if "" == k {
			continue
		}
		_, exist := fieldMap[k]
		if false == exist {
			return nil, fmt.Errorf("field '%s' not recognized", k)
		}
		switch v.(type) {
		case int, int64, int32, int16, int8:
			n := reflect.ValueOf(v).Int()
			kv = append(kv, fmt.Sprintf("`%s` = %d", k, n))
		case uint, uint64, uint32, uint16, uint8:
			u := reflect.ValueOf(v).Uint()
			kv = append(kv, fmt.Sprintf("`%s` = %d", k, u))
		case bool:
			if v.(bool) {
				kv = append(kv, "`"+k+"`"+" = TRUE")
			} else {
				kv = append(kv, "`"+k+"`"+" = FALSE")
			}
		case float32, float64:
			f := reflect.ValueOf(v).Float()
			kv = append(kv, fmt.Sprintf("`%s` = %f", k, f))
		case string:
			s := escapeValueString(v.(string))
			kv = append(kv, "`"+k+"`"+" = "+addQuoteToString(s, "'"))
		case time.Time:
			t := v.(time.Time)
			valStr := convTimeToString(t, fieldMap, k)
			kv = append(kv, "`"+k+"`"+" = "+valStr)
		case nil:
			kv = append(kv, "`"+k+"`"+" = NULL")
		case Raw:
			valStr := string(v.(Raw))
			kv = append(kv, "`"+k+"` "+valStr)
		}
	}
	return kv, nil
}
