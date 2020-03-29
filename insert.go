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
	_timeRegex     = regexp.MustCompile(`^time(\d)$`)
	_datetimeRegex = regexp.MustCompile(`^datetime(\d)$`)
)

// ========

// InsertFields return keys and values for inserting. Auto-increment fields will be ignored
func (d *DB) InsertFields(s interface{}, backQuoted bool) (keys []string, values []string, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	// read from buffer
	fieldMap, err := d.getFieldMap(s)
	if err != nil {
		return
	}
	// log.Println("field map:", fieldMap)

	// handle each fields
	numField := t.NumField()
	keys = make([]string, 0, numField)
	values = make([]string, 0, numField)
	// log.Printf("got %d field(s)\n", numField)

	for i := 0; i < numField; i++ {
		tf := t.Field(i) // *StructField
		vf := v.Field(i) // *Value
		if false == vf.CanInterface() {
			// log.Println(tf.Type, "cannot interface")
			continue
		}

		fieldName := getFieldName(&tf)
		if fieldName == "-" {
			continue
		}
		if fieldName == "" {
			if tf.Type.Kind() != reflect.Struct {
				// log.Println("skip field", fieldName, "type", tf.Type.Kind())
				continue // skip this
			}
		} else {
			f, exist := fieldMap[fieldName]
			if false == exist || f.AutoIncrement {
				// log.Println(fieldName, "not exists")
				continue
			}
		}

		var val string
		intf := vf.Interface()
		// log.Println("got field", fieldName)

		switch intf.(type) {
		case int, int8, int16, int32, int64:
			val = strconv.FormatInt(vf.Int(), 10)
		case uint, uint8, uint16, uint32, uint64:
			val = strconv.FormatUint(vf.Uint(), 10)
		case string:
			s := escapeValueString(vf.String())
			val = addQuoteToString(s, "'")
		case bool:
			val = convBoolToString(vf.Bool())
		case float32, float64:
			val = fmt.Sprintf("%f", vf.Float())
		case sql.NullString:
			val = convNullStringToString(intf.(sql.NullString), "'")
		case sql.NullInt64:
			val = convNullInt64ToString(intf.(sql.NullInt64))
		case sql.NullBool:
			val = convNullBoolToString(intf.(sql.NullBool))
		case sql.NullFloat64:
			val = convNullFloat64ToString(intf.(sql.NullFloat64))
		case mysql.NullTime:
			nt := intf.(mysql.NullTime)
			if nt.Valid {
				val = convTimeToString(nt.Time, fieldMap, fieldName)
			} else {
				val = "NULL"
			}
		case time.Time:
			val = convTimeToString(intf.(time.Time), fieldMap, fieldName)
		default:
			if reflect.Struct == tf.Type.Kind() {
				// log.Println("Embedded struct: ", tf.Type)
				embedKey, embedValue, err := d.InsertFields(vf.Interface(), false)
				if err != nil {
					return nil, nil, err
				}
				keys = append(keys, embedKey...)
				values = append(values, embedValue...)
			} else {
				// log.Println("unknown type:", tf.Type.Kind())
			}
			continue
		}

		keys = append(keys, fieldName)
		values = append(values, val)
		// continue
	}

	if backQuoted {
		for i, s := range keys {
			keys[i] = "`" + s + "`"
		}
	}

	return
}

// Insert insert a given structure. auto-increment fields will be ignored
func (d *DB) Insert(v interface{}, opts ...Options) (result sql.Result, err error) {
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

	err = d.checkAutoCreateTable(v, opt)
	if err != nil {
		return nil, err
	}

	// INSERT
	query := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", opt.TableName, strings.Join(keys, ", "), strings.Join(values, ", "))
	// log.Println(query)
	return d.db.Exec(query)
}

func convNullStringToString(s sql.NullString, quote string) string {
	if false == s.Valid {
		return "NULL"
	}
	return addQuoteToString(s.String, quote)
}

func convNullInt64ToString(n sql.NullInt64) string {
	if false == n.Valid {
		return "NULL"
	}
	return strconv.FormatInt(n.Int64, 10)
}

func convNullBoolToString(b sql.NullBool) string {
	if false == b.Valid {
		return "NULL"
	} else if b.Bool {
		return "TRUE"
	} else {
		return "FALSE"
	}
}

func convNullFloat64ToString(f sql.NullFloat64) string {
	if false == f.Valid {
		return "NULL"
	}
	return fmt.Sprintf("%f", f.Float64)
}

func convBoolToString(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func addQuoteToString(s, quote string) string {
	if quote == `"` {
		return `"` + strings.Replace(s, "\"", "\\\"", -1) + `"`
	}
	return `'` + strings.Replace(s, "'", "\\'", -1) + `'`
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
		if sub := _datetimeRegex.FindStringSubmatch(ty); sub != nil && len(sub) > 0 {
			count, _ := strconv.Atoi(sub[0])
			if 0 == count {
				return t.Format("'2006-01-02 15:04:05'")
			}
			return t.Format("'2006-01-02 15:04:05." + strings.Repeat("0", count) + "'")

		} else if sub := _timeRegex.FindStringSubmatch(ty); sub != nil && len(sub) > 0 {
			count, _ := strconv.Atoi(sub[0])
			if 0 == count {
				return t.Format("'15:04:05'")
			}
			return t.Format("'15:04:05." + strings.Repeat("0", count) + "'")

		} else {
			return "NULL"
		}
	}
}
