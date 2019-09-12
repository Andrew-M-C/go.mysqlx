package mysqlx

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	_time_regex     = regexp.MustCompile(`^time(\d)$`)
	_datetime_regex = regexp.MustCompile(`^datetime(\d)$`)
)

// tools for MySQL SELECT
func (d *DB) SelectFields(s interface{}) (string, error) {
	// read from buffer
	intf_name := reflect.TypeOf(s)
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
			// log.Println(tf.Type, " cannot interface")
			continue
		}

		field_name := getFieldName(&tf)
		if field_name == "" || field_name == "-" {
			if tf.Type.Kind() != reflect.Struct {
				continue // skip this
			}
		}

		switch tf.Type.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			f, _ := field_map[field_name]
			if false == f.AutoIncrement {
				keys = append(keys, "`"+field_name+"`")
				values = append(values, strconv.FormatInt(vf.Int(), 10))
			}
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			f, _ := field_map[field_name]
			if false == f.AutoIncrement {
				keys = append(keys, "`"+field_name+"`")
				values = append(values, strconv.FormatUint(vf.Uint(), 10))
			}
		case reflect.String:
			keys = append(keys, "`"+field_name+"`")
			values = append(values, addQuoteToString(vf.String(), "'"))
		case reflect.Bool:
			keys = append(keys, "`"+field_name+"`")
			if vf.Bool() {
				values = append(values, "TRUE")
			} else {
				values = append(values, "FALSE")
			}
		case reflect.Float32, reflect.Float64:
			keys = append(keys, "`"+field_name+"`")
			values = append(values, fmt.Sprintf("%f", vf.Float()))
		case reflect.Struct:
			switch tf.Type.String() {
			case "sql.NullString":
				keys = append(keys, "`"+field_name+"`")
				if vf.Field(1).Bool() {
					values = append(values, addQuoteToString(vf.Field(0).String(), "'"))
				} else {
					values = append(values, "NULL")
				}
			case "sql.NullInt64":
				keys = append(keys, "`"+field_name+"`")
				if vf.Field(1).Bool() {
					values = append(values, strconv.FormatInt(vf.Field(0).Int(), 10))
				} else {
					values = append(values, "NULL")
				}
			case "sql.NullBool":
				keys = append(keys, "`"+field_name+"`")
				if vf.Field(1).Bool() {
					if vf.Field(0).Bool() {
						values = append(values, "TRUE")
					} else {
						values = append(values, "FALSE")
					}
				} else {
					values = append(values, "NULL")
				}
			case "sql.NullFloat64":
				keys = append(keys, "`"+field_name+"`")
				if vf.Field(1).Bool() {
					values = append(values, fmt.Sprintf("%f", vf.Field(0).Float()))
				} else {
					values = append(values, "NULL")
				}
			case "mysql.NullTime":
				keys = append(keys, "`"+field_name+"`")
				if vf.Field(1).Bool() {
					values = append(values, convTimeToString(vf.Field(0).Interface().(time.Time), field_map, field_name))
				} else {
					values = append(values, "NULL")
				}
			case "time.Time":
				keys = append(keys, "`"+field_name+"`")
				values = append(values, convTimeToString(vf.Interface().(time.Time), field_map, field_name))
			default:
				// log.Println("Embedded struct: ", tf.Type)
				embed_key, embed_value, err := d.InsertFields(vf.Interface())
				if err != nil {
					return nil, nil, err
				}
				keys = append(keys, embed_key...)
				values = append(values, embed_value...)
			}
		}
	}

	return
}

func (d *DB) Insert(v interface{}, opts ...Options) (lastInsertId int64, err error) {
	if nil == d.db {
		return -1, fmt.Errorf("nil *sqlx.DB")
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
	// log.Println(query)
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
