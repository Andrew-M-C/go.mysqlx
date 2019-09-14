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

// tools for MySQL SELECT
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

	// log.Println(fields_str)
	intf_name := reflect.TypeOf(prototype)
	var field_map map[string]*Field
	if field_map_value, exist := d.bufferedFieldMaps.Load(intf_name); exist {
		field_map = field_map_value.(map[string]*Field)
	} else {
		field_map = make(map[string]*Field)
		fields, err := d.ReadStructFields(prototype)
		if err != nil {
			return err
		}

		for _, f := range fields {
			field_map[f.Name] = f
		}
		d.bufferedFieldMaps.Store(intf_name, field_map)
	}

	// parse arguments
	opt := mergeOptions(prototype)
	offset := 0
	limit := 0
	cond_list := make([]string, 0, len(args))
	order_list := make([]string, 0, len(args))

	for _, arg := range args {
		switch arg.(type) {
		default:
			t := reflect.TypeOf(arg)
			return fmt.Errorf("unsupported type %v", t)
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
			c := packCond(&cond, field_map)
			if "" != c {
				cond_list = append(cond_list, c)
			}
		case *Cond:
			cond := arg.(*Cond)
			c := packCond(cond, field_map)
			if "" != c {
				cond_list = append(cond_list, c)
			}
		case Order:
			order := arg.(Order)
			o := packOrder(&order)
			if "" != o {
				order_list = append(order_list, o)
			}
		case *Order:
			order := arg.(*Order)
			o := packOrder(order)
			if "" != o {
				order_list = append(order_list, o)
			}
		}
	}

	// final arguments check
	if "" == opt.TableName {
		return fmt.Errorf("nil table name")
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

func packCond(c *Cond, fieldMap map[string]*Field) string {
	if c.Param == "" {
		return ""
	}

	c.Operator = strings.ToUpper(c.Operator)
	switch c.Operator {
	case "==":
		c.Operator = "="
	case "=", "!=", "<>", ">", "<", ">=", "<=", "IS", "IS NOT":
		// OK
	default:
		return ""
	}

	var val_str string
	switch c.Value.(type) {
	case int, int64, int32, int16, int8:
		n := reflect.ValueOf(c.Value).Int()
		val_str = strconv.FormatInt(n, 10)
	case uint, uint64, uint32, uint16, uint8:
		n := reflect.ValueOf(c.Value).Uint()
		val_str = strconv.FormatUint(n, 10)
	case bool:
		if c.Value.(bool) {
			val_str = "TRUE"
		} else {
			val_str = "FALSE"
		}
	case float32, float64:
		f := reflect.ValueOf(c.Value).Float()
		val_str = fmt.Sprintf("%f", f)
	case string:
		s := c.Value.(string)
		val_str = addQuoteToString(s, "'")
	case time.Time:
		t := c.Value.(time.Time)
		_, exist := fieldMap[c.Param]
		if false == exist {
			return ""
		}
		val_str = convTimeToString(t, fieldMap, c.Param)
	case nil:
		switch c.Operator {
		case "=":
			c.Operator = "IS"
		case "!=":
			c.Operator = "IS NOT"
		default:
			// do nothing
		}
		val_str = "NULL"
	}

	return fmt.Sprintf("`%s` %s %s", c.Param, c.Operator, val_str)
}

// tools for MySQL INSERT
func (d *DB) InsertFields(s interface{}) (keys []string, values []string, err error) {
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
				keys = append(keys, "`"+field_name+"`")
				values = append(values, strconv.FormatInt(vf.Int(), 10))
			}
		case uint, uint8, uint16, uint32, uint64:
			f, _ := field_map[field_name]
			if false == f.AutoIncrement {
				keys = append(keys, "`"+field_name+"`")
				values = append(values, strconv.FormatUint(vf.Uint(), 10))
			}
		case string:
			keys = append(keys, "`"+field_name+"`")
			values = append(values, addQuoteToString(vf.String(), "'"))
		case bool:
			keys = append(keys, "`"+field_name+"`")
			if vf.Bool() {
				values = append(values, "TRUE")
			} else {
				values = append(values, "FALSE")
			}
		case float32, float64:
			keys = append(keys, "`"+field_name+"`")
			values = append(values, fmt.Sprintf("%f", vf.Float()))
		case sql.NullString:
			ns := intf.(sql.NullString)
			keys = append(keys, "`"+field_name+"`")
			if ns.Valid {
				values = append(values, addQuoteToString(ns.String, "'"))
			} else {
				values = append(values, "NULL")
			}
		case sql.NullInt64:
			ni := intf.(sql.NullInt64)
			keys = append(keys, "`"+field_name+"`")
			if ni.Valid {
				values = append(values, strconv.FormatInt(ni.Int64, 10))
			} else {
				values = append(values, "NULL")
			}
		case sql.NullBool:
			nb := intf.(sql.NullBool)
			keys = append(keys, "`"+field_name+"`")
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
			keys = append(keys, "`"+field_name+"`")
			if vf.Field(1).Bool() {
				values = append(values, fmt.Sprintf("%f", nf.Float64))
			} else {
				values = append(values, "NULL")
			}
		case mysql.NullTime:
			nt := intf.(mysql.NullTime)
			keys = append(keys, "`"+field_name+"`")
			if nt.Valid {
				values = append(values, convTimeToString(nt.Time, field_map, field_name))
			} else {
				values = append(values, "NULL")
			}
		case time.Time:
			t := intf.(time.Time)
			keys = append(keys, "`"+field_name+"`")
			values = append(values, convTimeToString(t, field_map, field_name))
		default:
			if reflect.Struct == tf.Type.Kind() {
				// // log.Println("Embedded struct: ", tf.Type)
				embed_key, embed_value, err := d.InsertFields(vf.Interface())
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

	return
}

func (d *DB) Insert(v interface{}, opts ...Options) (lastInsertId int64, err error) {
	if nil == d.db {
		return -1, fmt.Errorf("nil *sqlx.DB")
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
		return -1, fmt.Errorf("parameter type invalid (%v)", ty)
	}

	keys, values, err := d.InsertFields(v)
	if err != nil {
		return -1, err
	}

	opt := mergeOptions(v, opts...)
	if "" == opt.TableName {
		return -1, fmt.Errorf("empty table name for type %v", reflect.TypeOf(v))
	}

	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", opt.TableName, strings.Join(keys, ", "), strings.Join(values, ", "))
	// // log.Println(query)
	res, err := d.db.Exec(query)
	if err != nil {
		return -1, err
	}
	return res.LastInsertId()
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
