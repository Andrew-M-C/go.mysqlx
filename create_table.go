package mysqlx

import (
	"bytes"
	"fmt"

	"reflect"
	"strings"
)

type OptionInterface interface {
	Options() Options
}

type category int

const (
	Integer category = iota
	Float
	String
	DateTime
	Bool
)

func convFieldListToMap(list []*Field) map[string]*Field {
	ret := make(map[string]*Field)
	for _, f := range list {
		ret[f.Name] = f
	}
	return ret
}

func (d *DB) MustCreateTable(v interface{}, opts ...Options) {
	err := d.CreateTable(v, opts...)
	if err != nil {
		panic(err)
	}
	return
}

func mergeOptions(v interface{}, opts ...Options) Options {
	opt := Options{}
	if intf, ok := v.(OptionInterface); ok {
		opt = intf.Options()
	}
	if len(opts) > 0 {
		// copy each option
		if "" != opts[0].TableName {
			opt.TableName = opts[0].TableName
		}
		if "" != opts[0].TableDescption {
			opt.TableDescption = opts[0].TableDescption
		}
		if opts[0].Indexes != nil && len(opts[0].Indexes) > 0 {
			opt.Indexes = opts[0].Indexes
		}
		if opts[0].Uniques != nil && len(opts[0].Uniques) > 0 {
			opt.Uniques = opts[0].Uniques
		}
	}
	return opt
}

func (d *DB) CreateTable(v interface{}, opts ...Options) error {
	if nil == d.db {
		return fmt.Errorf("mysqlx not initialized")
	}

	// read options
	opt := mergeOptions(v, opts...)
	// log.Println("final opts: ", opt)

	// check options
	if "" == opt.TableName {
		return fmt.Errorf("table name not specified")
	}

	// read fields
	fields, err := ReadStructFields(v)
	if err != nil {
		return err
	}

	prev_field_map := make(map[string]*Field) // used for AFTER section in ALTER statements
	var prev_field *Field
	for _, f := range fields {
		prev_field_map[f.Name] = prev_field
		prev_field = f
	}

	should_create := false

	// Firstly, read fields and check if table exists
	fields_in_db, err := d.ReadTableFields(opt.TableName)
	if err != nil {
		if false == strings.Contains(err.Error(), "doesn't exist") {
			return err
		} else {
			should_create = true
			// and then continue
		}
	}

	// create or alter fields
	if should_create || nil == fields_in_db {
		// create table
		var auto_inc_field *Field
		statements := make([]string, 0, len(fields)+len(opt.Indexes)+len(opt.Uniques)+1)

		// make fields statements
		for _, f := range fields {
			comment := strings.Replace(f.Comment, "'", "\\'", -1)
			null := ""
			if false == f.Nullable {
				null = "NOT NULL"
			}
			if f.AutoIncrement {
				auto_inc_field = f
				f.statement = fmt.Sprintf("`%s` %s %s AUTO_INCREMENT COMMENT '%s'", f.Name, f.Type, null, comment)
			} else {
				f.statement = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT '%s'", f.Name, f.Type, null, f.Default, comment)
			}

			// log.Printf("statement: %s\n", f.statement)
			statements = append(statements, f.statement)
		}

		// make index statements
		if auto_inc_field != nil {
			s := fmt.Sprintf("PRIMARY KEY (`%s`)", auto_inc_field.Name)
			statements = append(statements, s)
			// log.Printf("statement: %s\n", s)
		}

		if opt.Indexes != nil && len(opt.Indexes) > 0 {
			for _, idx := range opt.Indexes {
				if err := idx.Check(); err != nil {
					return err
				}

				idx_field_list := make([]string, 0, len(idx.Fields))
				for _, f := range idx.Fields {
					idx_field_list = append(idx_field_list, "`"+f+"`")
				}
				s := fmt.Sprintf("KEY `%s` (%s)", idx.Name, strings.Join(idx_field_list, ", "))
				statements = append(statements, s)
				// log.Printf("statememt: %s\n", s)
			}
		}

		// make unique statements
		if opt.Uniques != nil && len(opt.Uniques) > 0 {
			for _, uniq := range opt.Uniques {
				if err := uniq.Check(); err != nil {
					return err
				}

				uniq_field_list := make([]string, 0, len(uniq.Fields))
				for _, f := range uniq.Fields {
					uniq_field_list = append(uniq_field_list, "`"+f+"`")
				}

				s := fmt.Sprintf("UNIQUE KEY `%s` (%s)", uniq.Name, strings.Join(uniq_field_list, ", "))

				statements = append(statements, s)
				// log.Printf("statememt: %s\n", s)
			}
		}

		// package final create statements
		desc := strings.Replace(opt.TableDescption, "'", "\\'", -1)
		auto_inc_1 := "AUTO_INCREMENT=1"
		if nil == auto_inc_field {
			auto_inc_1 = ""
		}
		final := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) ENGINE=InnoDB %s DEFAULT CHARSET=utf8 COMMENT '%s'",
			opt.TableName,
			strings.Join(statements, ",\n"),
			auto_inc_1, desc,
		)

		// exec
		// log.Println(final)
		_, err := d.db.Exec(final)
		if err != nil {
			return err
		} else {
			return nil
		}

	} else {
		// check and alter fields
		// find missing columns and then alter them
		fields_in_db_map := convFieldListToMap(fields_in_db)
		for _, f := range fields {
			_, exist := fields_in_db_map[f.Name]
			if exist {
				continue
			}

			// new primary key is not allowed
			if f.AutoIncrement {
				return fmt.Errorf("new promary key `%s` (auto increment) is not allowed", f.Name)
			}

			// not exist? should ALTER
			var func_insert_field func(*Field) error
			func_insert_field = func(f *Field) error {
				comment := strings.Replace(f.Comment, "'", "\\'", -1)
				null := ""
				if false == f.Nullable {
					null = "NOT NULL"
				}

				prev_field, _ := prev_field_map[f.Name]
				if prev_field == nil {
					// this is first column
					statement := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' FIRST", opt.TableName, f.Name, f.Type, null, f.Default, comment)
					// log.Println(statement)
					_, err := d.db.Exec(statement)
					if err != nil {
						return err
					} else {
						fields_in_db_map[f.Name] = f
						return nil
					}
				} else {
					_, prev_exist_in_db := fields_in_db_map[prev_field.Name]
					if false == prev_exist_in_db {
						// previous map has not been inserted
						err := func_insert_field(prev_field)
						if err != nil {
							return err
						}
					} else {
						statement := fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' AFTER `%s`", opt.TableName, f.Name, f.Type, null, f.Default, comment, prev_field.Name)
						// log.Println(statement)
						_, err := d.db.Exec(statement)
						if err != nil {
							return err
						} else {
							fields_in_db_map[f.Name] = f
							return nil
						}
					}
				}
				return nil
			}
			err := func_insert_field(f)
			if err != nil {
				return err
			}
		}

		// read index and uniques
		index_in_db, uniq_in_db, err := d.ReadTableIndexes(opt.TableName)
		if err != nil {
			return err
		}

		// add indexes
		if opt.Indexes != nil && len(opt.Indexes) > 0 {
			for _, idx := range opt.Indexes {
				if err := idx.Check(); err != nil {
					return err
				}

				if _, exist := index_in_db[idx.Name]; exist {
					continue
				}

				idx_field_list := make([]string, 0, len(idx.Fields))
				for _, f := range idx.Fields {
					idx_field_list = append(idx_field_list, "`"+f+"`")
				}
				s := fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", opt.TableName, idx.Name, strings.Join(idx_field_list, ", "))

				_, err := d.db.Exec(s)
				if err != nil {
					return err
				}
			}
		}

		// add uniques
		if opt.Uniques != nil && len(opt.Uniques) > 0 {
			for _, uniq := range opt.Uniques {
				if err := uniq.Check(); err != nil {
					return err
				}

				if _, exist := uniq_in_db[uniq.Name]; exist {
					continue
				}

				uniq_field_list := make([]string, 0, len(uniq.Fields))
				for _, f := range uniq.Fields {
					uniq_field_list = append(uniq_field_list, "`"+f+"`")
				}
				s := fmt.Sprintf("ALTER TABLE `%s` ADD UNIQUE `%s` (%s)", opt.TableName, uniq.Name, strings.Join(uniq_field_list, ", "))

				_, err := d.db.Exec(s)
				if err != nil {
					return err
				}
			}
		}
		// TODO:

		return nil
	}
}

func readStructFields(t reflect.Type, v reflect.Value) (ret []*Field, err error) {
	num_field := t.NumField()
	ret = make([]*Field, 0, num_field)

	for i := 0; i < num_field; i++ {
		tf := t.Field(i) // *StructField
		vf := v.Field(i) // *Value
		if false == vf.CanInterface() {
			// log.Println(tf.Type, " cannot interface")
			continue
		}

		field_name := getFieldName(&tf)
		field_type := ""
		field_null := false
		field_dflt := ""
		field_incr := false
		field_comt := ""

		if field_name == "" || field_name == "-" {
			if tf.Type.Kind() != reflect.Struct {
				continue // skip this
			}
		}

		// log.Printf("%02d - name %s, type: %v\n", i, tf.Name, tf.Type.Kind())
		switch tf.Type.Kind() {
		case reflect.Int64:
			field_type = getFieldType(&tf, "bigint")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Uint64:
			field_type = getFieldType(&tf, "bigint unsigned")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Int, reflect.Int32:
			field_type = getFieldType(&tf, "int")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Uint, reflect.Uint32:
			field_type = getFieldType(&tf, "int unsigned")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Int16:
			field_type = getFieldType(&tf, "smallint")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Uint16:
			field_type = getFieldType(&tf, "smallint unsigned")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Int8:
			field_type = getFieldType(&tf, "tinyint")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.Uint8:
			field_type = getFieldType(&tf, "tinyint unsigned")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Integer, field_null)
			field_incr = getFieldAutoIncrement(&tf, false)
			field_comt = getFieldComment(&tf)
		case reflect.String:
			field_type = getFieldType(&tf, "")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, String, field_null)
			field_incr = false
			field_comt = getFieldComment(&tf)
			if "" == field_type {
				return nil, fmt.Errorf("missing type tag for string field '%s'", field_name)
			}
		case reflect.Bool:
			field_type = getFieldType(&tf, "boolean")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Bool, field_null)
			field_incr = false
			field_comt = getFieldComment(&tf)
		case reflect.Float32, reflect.Float64:
			field_type = getFieldType(&tf, "")
			field_null = getFieldNullable(&tf, false)
			field_dflt = getFieldDefault(&tf, Float, field_null)
			field_incr = false
			field_comt = getFieldComment(&tf)
			if "" == field_type {
				return nil, fmt.Errorf("missing type tag for float field '%s'", field_name)
			}
		case reflect.Struct:
			switch tf.Type.String() {
			case "sql.NullString":
				field_type = getFieldType(&tf, "")
				field_null = getFieldNullable(&tf, true)
				field_dflt = getFieldDefault(&tf, String, field_null)
				field_incr = false
				field_comt = getFieldComment(&tf)
				if "" == field_type {
					return nil, fmt.Errorf("missing type tag for sql.NullString field '%s'", field_name)
				}
			case "sql.NullInt64":
				field_type = getFieldType(&tf, "bigint")
				field_null = getFieldNullable(&tf, true)
				field_dflt = getFieldDefault(&tf, Integer, field_null)
				field_incr = getFieldAutoIncrement(&tf, false)
				field_comt = getFieldComment(&tf)
			case "sql.NullBool":
				field_type = getFieldType(&tf, "boolean")
				field_null = getFieldNullable(&tf, true)
				field_dflt = getFieldDefault(&tf, Bool, field_null)
				field_incr = getFieldAutoIncrement(&tf, false)
				field_comt = getFieldComment(&tf)
			case "sql.NullFloat64":
				field_type = getFieldType(&tf, "")
				field_null = getFieldNullable(&tf, true)
				field_dflt = getFieldDefault(&tf, Float, field_null)
				field_incr = getFieldAutoIncrement(&tf, false)
				field_comt = getFieldComment(&tf)
				if "" == field_type {
					return nil, fmt.Errorf("missing type tag for sql.NullFloat64 field '%s'", field_name)
				}
			case "mysql.NullTime":
				field_type = getFieldType(&tf, "datetime")
				field_null = getFieldNullable(&tf, true)
				field_dflt = getFieldDefault(&tf, DateTime, field_null, field_type)
				field_incr = getFieldAutoIncrement(&tf, false)
				field_comt = getFieldComment(&tf)
			case "time.Time":
				field_type = getFieldType(&tf, "datetime")
				field_null = getFieldNullable(&tf, false)
				field_dflt = getFieldDefault(&tf, DateTime, field_null, field_type)
				field_incr = getFieldAutoIncrement(&tf, false)
				field_comt = getFieldComment(&tf)
			default:
				// log.Println("Embedded struct: ", tf.Type)
				sub_fields, err := readStructFields(tf.Type, vf)
				if err != nil {
					return nil, err
				}
				ret = append(ret, sub_fields...)
				continue
			}
		default:
			// log.Printf("unrecognized type %v\n", tf.Type.Kind())
			continue
			// go on below
		}

		// done
		// // log.Printf("%02d - Got tag name: %s\n", i, field_name)
		// // log.Printf("     Got tag type: %s\n", field_type)
		// // log.Printf("     Got tag null: %v\n", field_null)
		// // log.Printf("     Got tag dflt: %v\n", field_dflt)
		// // log.Printf("     Got tag incr: %v\n", field_incr)
		// // log.Printf("     Got tag comt: %v\n", field_comt)

		ret = append(ret, &Field{
			Name:          field_name,
			Type:          field_type,
			Nullable:      field_null,
			Default:       field_dflt,
			AutoIncrement: field_incr,
			Comment:       field_comt,
		})
	}
	return
}

func getFieldName(tf *reflect.StructField) string {
	db_tag_list := strings.SplitN(tf.Tag.Get("db"), ",", 2)
	if nil == db_tag_list || 0 == len(db_tag_list) || "" == db_tag_list[0] {
		// no db tags, this field would be ignored
		// return _fieldNameToSql(tf.Name)
		return ""
	} else {
		return db_tag_list[0]
	}
}

func getFieldType(tf *reflect.StructField, dft string) string {
	t := _readMysqlxTag(tf, "type")
	if "" == t {
		return dft
	} else {
		if 'u' == t[0] {
			return t[1:] + " unsigned"
		} else {
			return t
		}
	}
}

func getFieldNullable(tf *reflect.StructField, dft bool) bool {
	n := _readMysqlxTag(tf, "null")
	switch n {
	case "true", "1":
		return true
	case "false", "0":
		return false
	default:
		return dft
	}
}

func getFieldAutoIncrement(tf *reflect.StructField, dft bool) bool {
	n := _readMysqlxTag(tf, "increment")
	switch n {
	case "true", "1":
		return true
	case "false", "0":
		return false
	default:
		return dft
	}
}

func getFieldDefault(tf *reflect.StructField, category category, nullable bool, fieldTypes ...string) string {
	n := _readMysqlxTag(tf, "default")
	if n != "" {
		switch category {
		case String:
			return "'" + strings.Replace(n, "'", "\\'", -1) + "'"
		case DateTime:
			if n == "0" {
				return "0"
			} else {
				return "'" + strings.Replace(n, "_", " ", -1) + "'"
			}
		default:
			return n
		}

	} else {
		if nullable {
			return "NULL"
		}
		switch category {
		case String:
			return "''"
		case Integer, Float:
			return "0"
		case Bool:
			return "FALSE"
		case DateTime:
			switch strings.ToLower(fieldTypes[0]) {
			case "timestamp":
				// return "convert_tz('1970-01-01 00:00:01', '+00:00', @@time_zone)"
				return "'1970-01-02 00:00:01'" // advoid timezone offsets
			case "datetime":
				return "'1970-01-01 00:00:00'"
			case "date":
				return "'1970-01-01'"
			case "time":
				return "'00:00:00'"
			case "year":
				return "'1970-01-01'"
			default:
				return "'1970-01-01'"
			}
		default:
			return "NULL"
		}
	}
}

func getFieldComment(tf *reflect.StructField) string {
	full_tag := tf.Tag.Get("comment")
	return strings.Trim(full_tag, " \t")
}

// tools only for this file
func _fieldNameToSql(field string) string {
	buff := bytes.Buffer{}
	for _, c := range field {
		if c >= 'A' && c <= 'Z' {
			buff.WriteRune('_')
			buff.WriteRune(c + ('a' - 'A'))
		} else {
			buff.WriteRune(c)
		}
	}

	ret := buff.String()
	return strings.Trim(ret, "_")
}

func _readMysqlxTag(tf *reflect.StructField, key string) (value string) {
	full_tag := tf.Tag.Get("mysqlx")
	kv_str_parts := strings.Split(full_tag, " ")

	// search for the key
	for _, s := range kv_str_parts {
		kv := strings.SplitN(s, ":", 2)
		if nil == kv || len(kv) < 2 {
			continue
		}

		k := strings.Trim(kv[0], " \t")
		v := strings.Trim(kv[1], " \t")
		if k == key {
			return v
		}
	}
	// not found
	return ""
}
