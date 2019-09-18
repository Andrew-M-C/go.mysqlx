package mysqlx

import (
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

var (
	_time_regex     = regexp.MustCompile(`^time(\d)$`)
	_datetime_regex = regexp.MustCompile(`^datetime(\d)$`)
)

// ========
// SELECT
func (d *DB) SelectFields(s interface{}) (string, error) {
	// TODO: read interface until we get a struct

	// read from buffer
	intf_name := reflect.TypeOf(s)
	// log.Printf("select type %v", intf_name)
	field_value, exist := d.bufferedSelectFields.Load(intf_name)
	if exist {
		return field_value.(string), nil
	}

	fields, err := d.ReadStructFields(s)
	if err != nil {
		return "", err
	}

	field_names := make([]string, 0, len(fields))
	for _, f := range fields {
		field_names = append(field_names, "`"+f.Name+"`")
	}

	ret := strings.Join(field_names, ", ")
	d.bufferedSelectFields.Store(intf_name, ret)
	return ret, nil
}

func (d *DB) getFieldMap(prototype interface{}) (fieldMap map[string]*Field, err error) {
	intf_name := reflect.TypeOf(prototype)
	if field_map_value, exist := d.bufferedFieldMaps.Load(intf_name); exist {
		fieldMap = field_map_value.(map[string]*Field)
	} else {
		fieldMap = make(map[string]*Field)
		var fields []*Field
		fields, err = d.ReadStructFields(prototype)
		if err != nil {
			return
		}

		for _, f := range fields {
			fieldMap[f.Name] = f
		}
		d.bufferedFieldMaps.Store(intf_name, fieldMap)
	}
	return
}

func (d *DB) getIncrementField(prototype interface{}) (field *Field, err error) {
	intf_name := reflect.TypeOf(prototype)
	if field_value, exist := d.bufferedIncrField.Load(intf_name); exist {
		field = field_value.(*Field)
		return
	} else {
		var fields []*Field
		fields, err = d.ReadStructFields(prototype)
		if err != nil {
			return
		}

		for _, f := range fields {
			if f.AutoIncrement {
				d.bufferedIncrField.Store(intf_name, f)
				return f, nil
			}
		}

		return nil, fmt.Errorf("'%s' has no increment field", intf_name)
	}
}

func (d *DB) handleArgs(prototype interface{}, args []interface{}) (
	fieldMap map[string]*Field, opt Options, offset int, limit int, condList []string, orderList []string, err error) {

	fieldMap, err = d.getFieldMap(prototype)
	if err != nil {
		return
	}
	opt = mergeOptions(prototype)
	condList = make([]string, 0, len(args))
	orderList = make([]string, 0, len(args))

	for _, arg := range args {
		switch arg.(type) {
		default:
			t := reflect.TypeOf(arg)
			err = fmt.Errorf("unsupported type %v", t)
			return
		case *Options:
			opt = mergeOptions(prototype, *(arg.(*Options)))
		case Options:
			opt = mergeOptions(prototype, arg.(Options))
		case Limit:
			limit = arg.(Limit).Limit
		case *Limit:
			limit = arg.(*Limit).Limit
		case Offset:
			offset = arg.(Offset).Offset
		case *Offset:
			offset = arg.(*Offset).Offset
		case Cond:
			cond := arg.(Cond)
			c := packCond(&cond, fieldMap)
			if "" != c {
				condList = append(condList, c)
			}
		case *Cond:
			cond := arg.(*Cond)
			c := packCond(cond, fieldMap)
			if "" != c {
				condList = append(condList, c)
			}
		case Order:
			order := arg.(Order)
			o := packOrder(&order)
			if "" != o {
				orderList = append(orderList, o)
			}
		case *Order:
			order := arg.(*Order)
			o := packOrder(order)
			if "" != o {
				orderList = append(orderList, o)
			}
		}
	}

	if "" == opt.TableName {
		err = fmt.Errorf("nil table name")
		return
	}
	return
}

func (d *DB) Select(dst interface{}, args ...interface{}) error {
	if nil == d.db {
		return fmt.Errorf("mysqlx not initialized")
	}

	// Should be *[]Xxx or *[]*Xxx
	ty := reflect.TypeOf(dst)
	va := reflect.ValueOf(dst)
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Ptr != ty.Kind() {
		return fmt.Errorf("parameter type invalid (%v)", ty)
	}

	// Should be []Xxx or []*Xxx
	va = va.Elem()
	ty = va.Type()
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Slice != ty.Kind() {
		return fmt.Errorf("first parameter type invalid (%v)", ty)
	}

	// Should be Xxx or *Xxx
	ty = ty.Elem()
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Struct != ty.Kind() {
		ty = ty.Elem()
		// log.Printf("%v - %v\n", ty, ty.Kind())
	}

	// Should be Xxx
	prototype := reflect.New(ty).Elem().Interface()
	fields_str, err := d.SelectFields(prototype)
	if err != nil {
		// log.Printf("read fields failed: %v", err)
		return err
	}

	// parse arguments
	_, opt, offset, limit, cond_list, order_list, err := d.handleArgs(prototype, args)
	if err != nil {
		return err
	}

	// pack SELECT statements
	var offset_str string
	if offset > 0 {
		offset_str = fmt.Sprintf("OFFSET %d", offset)
	}

	var limit_str string
	if limit > 0 {
		limit_str = fmt.Sprintf("LIMIT %d", limit)
	}

	var order_str string
	if len(order_list) > 0 {
		order_str = "ORDER BY " + strings.Join(order_list, ", ")
	}

	var cond_str string
	if len(cond_list) > 0 {
		cond_str = "WHERE " + strings.Join(cond_list, " AND ")
	}

	query := fmt.Sprintf(
		"SELECT %s FROM `%s` %s %s %s %s",
		fields_str, opt.TableName, cond_str, order_str, limit_str, offset_str,
	)
	// log.Println(query)

	return d.db.Select(dst, query)
}

func packOrder(o *Order) string {
	if o.Param == "" {
		return ""
	}

	return fmt.Sprintf("`%s` %s", o.Param, o.Seq)
}

func parseCond(c *Cond, fieldMap map[string]*Field) (field, operator, value string, err error) {
	// param
	if c.Param == "" {
		err = fmt.Errorf("nil param name")
		return
	}

	// operator
	c.Operator = strings.ToUpper(c.Operator)
	switch c.Operator {
	case "==":
		c.Operator = "="
	case "=", "!=", "<>", ">", "<", ">=", "<=", "IS", "IS NOT":
		// OK
	default:
		err = fmt.Errorf("invalid operator '%s'", c.Operator)
		return
	}

	// value
	switch c.Value.(type) {
	case int, int64, int32, int16, int8:
		n := reflect.ValueOf(c.Value).Int()
		value = strconv.FormatInt(n, 10)
	case uint, uint64, uint32, uint16, uint8:
		n := reflect.ValueOf(c.Value).Uint()
		value = strconv.FormatUint(n, 10)
	case bool:
		if c.Value.(bool) {
			value = "TRUE"
		} else {
			value = "FALSE"
		}
	case float32, float64:
		f := reflect.ValueOf(c.Value).Float()
		value = fmt.Sprintf("%f", f)
	case string:
		s := c.Value.(string)
		value = addQuoteToString(s, "'")
	case time.Time:
		t := c.Value.(time.Time)
		_, exist := fieldMap[c.Param]
		if false == exist {
			err = fmt.Errorf("field '%s' not found", c.Param)
			return
		}
		value = convTimeToString(t, fieldMap, c.Param)
	case nil:
		switch c.Operator {
		case "=", "==":
			c.Operator = "IS"
		case "!=":
			c.Operator = "IS NOT"
		default:
			// do nothing
		}
		value = "NULL"
	}

	// return
	field = c.Param
	operator = c.Operator
	return
}

func packCond(c *Cond, fieldMap map[string]*Field) string {
	field, operator, value, err := parseCond(c, fieldMap)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("`%s` %s %s", field, operator, value)
}

// ========
// INSERT
func (d *DB) InsertFields(s interface{}, backQuoted bool) (keys []string, values []string, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	// read from buffer
	intf_name := reflect.TypeOf(s)
	var field_map map[string]*Field
	if field_map_value, exist := d.bufferedFieldMaps.Load(intf_name); exist {
		field_map = field_map_value.(map[string]*Field)
	} else {
		field_map = make(map[string]*Field)
		fields, err := d.ReadStructFields(s)
		if err != nil {
			return nil, nil, err
		}

		for _, f := range fields {
			field_map[f.Name] = f
		}
		d.bufferedFieldMaps.Store(intf_name, field_map)
	}

	// handle each fields
	num_field := t.NumField()
	keys = make([]string, 0, num_field)
	values = make([]string, 0, num_field)

	for i := 0; i < num_field; i++ {
		tf := t.Field(i) // *StructField
		vf := v.Field(i) // *Value
		if false == vf.CanInterface() {
			// // log.Println(tf.Type, " cannot interface")
			continue
		}

		field_name := getFieldName(&tf)
		if field_name == "" || field_name == "-" {
			if tf.Type.Kind() != reflect.Struct {
				continue // skip this
			}
		}

		intf := vf.Interface()
		switch intf.(type) {
		case int, int8, int16, int32, int64:
			f, _ := field_map[field_name]
			if false == f.AutoIncrement {
				keys = append(keys, field_name)
				values = append(values, strconv.FormatInt(vf.Int(), 10))
			}
		case uint, uint8, uint16, uint32, uint64:
			f, _ := field_map[field_name]
			if false == f.AutoIncrement {
				keys = append(keys, field_name)
				values = append(values, strconv.FormatUint(vf.Uint(), 10))
			}
		case string:
			keys = append(keys, field_name)
			values = append(values, addQuoteToString(vf.String(), "'"))
		case bool:
			keys = append(keys, field_name)
			if vf.Bool() {
				values = append(values, "TRUE")
			} else {
				values = append(values, "FALSE")
			}
		case float32, float64:
			keys = append(keys, field_name)
			values = append(values, fmt.Sprintf("%f", vf.Float()))
		case sql.NullString:
			ns := intf.(sql.NullString)
			keys = append(keys, field_name)
			if ns.Valid {
				values = append(values, addQuoteToString(ns.String, "'"))
			} else {
				values = append(values, "NULL")
			}
		case sql.NullInt64:
			ni := intf.(sql.NullInt64)
			keys = append(keys, field_name)
			if ni.Valid {
				values = append(values, strconv.FormatInt(ni.Int64, 10))
			} else {
				values = append(values, "NULL")
			}
		case sql.NullBool:
			nb := intf.(sql.NullBool)
			keys = append(keys, field_name)
			if nb.Valid {
				if nb.Bool {
					values = append(values, "TRUE")
				} else {
					values = append(values, "FALSE")
				}
			} else {
				values = append(values, "NULL")
			}
		case sql.NullFloat64:
			nf := intf.(sql.NullFloat64)
			keys = append(keys, field_name)
			if vf.Field(1).Bool() {
				values = append(values, fmt.Sprintf("%f", nf.Float64))
			} else {
				values = append(values, "NULL")
			}
		case mysql.NullTime:
			nt := intf.(mysql.NullTime)
			keys = append(keys, field_name)
			if nt.Valid {
				values = append(values, convTimeToString(nt.Time, field_map, field_name))
			} else {
				values = append(values, "NULL")
			}
		case time.Time:
			t := intf.(time.Time)
			keys = append(keys, field_name)
			values = append(values, convTimeToString(t, field_map, field_name))
		default:
			if reflect.Struct == tf.Type.Kind() {
				// // log.Println("Embedded struct: ", tf.Type)
				embed_key, embed_value, err := d.InsertFields(vf.Interface(), false)
				if err != nil {
					return nil, nil, err
				}
				keys = append(keys, embed_key...)
				values = append(values, embed_value...)
			} else {
				// ignore this field
			}
		}
	}

	if backQuoted {
		for i, s := range keys {
			keys[i] = "`" + s + "`"
		}
	}

	return
}

func (d *DB) Insert(v interface{}, opts ...Options) (result sql.Result, err error) {
	if nil == d.db {
		return nil, fmt.Errorf("nil *sqlx.DB")
	}

	// Should be *Xxx or Xxx
	ty := reflect.TypeOf(v)
	va := reflect.ValueOf(v)
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Ptr == ty.Kind() {
		v = va.Elem().Interface()
		ty = reflect.TypeOf(v)
		va = reflect.ValueOf(v)
	}

	if reflect.Struct != ty.Kind() {
		return nil, fmt.Errorf("parameter type invalid (%v)", ty)
	}

	keys, values, err := d.InsertFields(v, true)
	if err != nil {
		return nil, err
	}

	opt := mergeOptions(v, opts...)
	if "" == opt.TableName {
		return nil, fmt.Errorf("empty table name for type %v", reflect.TypeOf(v))
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", opt.TableName, strings.Join(keys, ", "), strings.Join(values, ", "))
	// // log.Println(query)
	return d.db.Exec(query)
}

func addQuoteToString(s, quote string) string {
	if quote == `"` {
		return `"` + strings.Replace(s, "\"", "\\\"", -1) + `"`
	} else {
		return `'` + strings.Replace(s, "'", "\\'", -1) + `'`
	}
}

func convTimeToString(t time.Time, fieldMap map[string]*Field, fieldName string) string {
	f, _ := fieldMap[fieldName]
	ty := strings.ToLower(f.Type)
	switch ty {
	case "timestamp", "datetime":
		return t.Format("'2006-01-02 15:04:05'")
	case "date":
		return t.Format("'2006-01-02'")
	case "time":
		return t.Format("'15:04:05'")
	case "year":
		return t.Format("'2006'")
	default:
		if sub := _datetime_regex.FindStringSubmatch(ty); sub != nil && len(sub) > 0 {
			count, _ := strconv.Atoi(sub[0])
			if 0 == count {
				return t.Format("'2006-01-02 15:04:05'")
			} else {
				return t.Format("'2006-01-02 15:04:05." + strings.Repeat("0", count) + "'")
			}
		} else if sub := _time_regex.FindStringSubmatch(ty); sub != nil && len(sub) > 0 {
			count, _ := strconv.Atoi(sub[0])
			if 0 == count {
				return t.Format("'15:04:05'")
			} else {
				return t.Format("'15:04:05." + strings.Repeat("0", count) + "'")
			}
		} else {
			return "NULL"
		}
	}
}

// ========
// UPDATE
func (d *DB) Update(prototype interface{}, fields map[string]interface{}, args ...interface{}) (sql.Result, error) {
	if nil == d.db {
		return nil, fmt.Errorf("nil *sqlx.DB")
	}
	if nil == fields || 0 == len(fields) {
		return nil, fmt.Errorf("nil fields")
	}

	// Should be *Xxx or Xxx
	ty := reflect.TypeOf(prototype)
	va := reflect.ValueOf(prototype)
	// log.Printf("%v - %v\n", ty, ty.Kind())
	if reflect.Ptr == ty.Kind() {
		prototype = va.Elem().Interface()
		ty = reflect.TypeOf(prototype)
		va = reflect.ValueOf(prototype)
	}

	if reflect.Struct != ty.Kind() {
		return nil, fmt.Errorf("parameter type invalid (%v)", ty)
	}

	intf_name := reflect.TypeOf(prototype)
	var field_map map[string]*Field
	if field_map_value, exist := d.bufferedFieldMaps.Load(intf_name); exist {
		field_map = field_map_value.(map[string]*Field)
	} else {
		field_map = make(map[string]*Field)
		fields, err := d.ReadStructFields(prototype)
		if err != nil {
			return nil, err
		}

		for _, f := range fields {
			field_map[f.Name] = f
		}
		d.bufferedFieldMaps.Store(intf_name, field_map)
	}

	opt := mergeOptions(prototype)
	kv := make([]string, 0, len(fields))
	cond_list := make([]string, 0, len(args))
	limit_str := ""
	cond_str := ""

	// handle fields
	for k, v := range fields {
		if "" == k {
			continue
		}
		_, exist := field_map[k]
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
			s := v.(string)
			kv = append(kv, "`"+k+"`"+" = "+addQuoteToString(s, "'"))
		case time.Time:
			t := v.(time.Time)
			val_str := convTimeToString(t, field_map, k)
			kv = append(kv, "`"+k+"`"+" = "+val_str)
		case nil:
			kv = append(kv, "`"+k+"`"+" = NULL")
		}
	}

	if 0 == len(kv) {
		return nil, fmt.Errorf("no value specified")
	}

	_, opt, _, limit, cond_list, _, err := d.handleArgs(prototype, args)
	if err != nil {
		return nil, err
	}
	if limit > 0 {
		limit_str = "LIMIT " + strconv.Itoa(limit)
	}

	if len(cond_list) > 0 {
		cond_str = "WHERE " + strings.Join(cond_list, " AND ")
	}
	query := fmt.Sprintf("UPDATE `%s` SET %s %s %s", opt.TableName, strings.Join(kv, ", "), cond_str, limit_str)
	// log.Println(query)

	return d.db.Exec(query)
}

// ========
// DELETE
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
	_, opt, _, limit, cond_list, order_list, err := d.handleArgs(prototype, args)
	if err != nil {
		return nil, err
	}

	// pack DELETE statements
	var limit_str string
	if limit > 0 {
		limit_str = fmt.Sprintf("LIMIT %d", limit)
	}

	var order_str string
	if len(order_list) > 0 {
		order_str = "ORDER BY " + strings.Join(order_list, ", ")
	}

	var cond_str string
	if len(cond_list) > 0 {
		cond_str = "WHERE " + strings.Join(cond_list, " AND ")
	}

	query := fmt.Sprintf(
		"DELETE FROM `%s` %s %s %s",
		opt.TableName, cond_str, order_str, limit_str,
	)
	// log.Println(query)

	return d.db.Exec(query)
}

// ========
func (d *DB) SelectOrInsertOne(insert interface{}, selectResult interface{}, conds ...interface{}) (res sql.Result, err error) {
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
	field_map, opt, _, _, cond_list, _, err := d.handleArgs(insert, conds)
	if err != nil {
		// log.Printf("handleArgs() failed: %v", err)
		return nil, err
	}
	if 0 == len(cond_list) {
		return nil, fmt.Errorf("select conditions not given")
	}
	if "" == opt.TableName {
		return nil, fmt.Errorf("nil table name")
	}

	// should have increment field
	incr_field, err := d.getIncrementField(insert)
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
	var first_list []string
	var second_list []string
	var third_list []string

	first_list = make([]string, len(keys))
	second_list = make([]string, len(keys))
	third_list = cond_list

	for i, k := range keys {
		v := values[i]
		first_list[i] = "`" + k + "`"
		second_list[i] = fmt.Sprintf("%s AS %s", v, addQuoteToString(k, "'"))
	}

	var random_field string
	for k, _ := range field_map {
		random_field = k
		break
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) SELECT * FROM (SELECT %s) AS tmp WHERE NOT EXISTS (SELECT `%s` FROM `%s` WHERE %s) LIMIT 1",
		opt.TableName, strings.Join(first_list, ", "), strings.Join(second_list, ", "), random_field, opt.TableName, strings.Join(third_list, " AND "),
	)
	// log.Println(query)

	// exec first
	res, err = d.db.Exec(query)
	if err != nil {
		return nil, err
	}

	if nil == selectResult {
		// simply return
		return res, nil
	}

	// determine insert status
	select_fields, err := d.SelectFields(insert)
	if err != nil {
		return res, err
	}
	insert_id, err := res.LastInsertId()
	if err != nil {
		// inserted, now select
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` = %d", select_fields, opt.TableName, incr_field.Name, insert_id)

	} else {
		// not inserted, just select as above
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", select_fields, opt.TableName, strings.Join(cond_list, " AND "))
	}

	// log.Println(query)
	return res, d.db.Select(selectResult, query)
}
