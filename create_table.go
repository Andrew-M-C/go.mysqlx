package mysqlx

import (
	"bytes"
	"database/sql"
	"fmt"
	"time"

	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type optionInterface interface {
	Options() Options
}

type category int

const (
	_Integer category = iota
	_Float
	_String
	_DateTime
	_Bool
)

func convFieldListToMap(list []*Field) map[string]*Field {
	ret := make(map[string]*Field)
	for _, f := range list {
		ret[f.Name] = f
	}
	return ret
}

// MustCreateTable is same as CreateTable. But is panics if error.
func (d *DB) MustCreateTable(v interface{}, opts ...Options) {
	err := d.CreateTable(v, opts...)
	if err != nil {
		panic(err)
	}
	return
}

// AutoCreateTable enables auto table creation. DB will check if table is created previously before
// each access to the DB.
//
// Please do NOT invoke this unless you need to automatically create table in runtime.
//
// Note 1: if a table was created once by mysqlx.DB after it was initialized, it will be noted as "created"
// and cached. Then mysqlx.DB will not check into MySQL DB again.
//
// Note 2: auto-table-creation will NOT be activated in Select() function!
func (d *DB) AutoCreateTable() {
	d.autoCreateTable.Store(true)
}

func (d *DB) checkAutoCreateTable(v interface{}, opt Options) error {
	if false == d.autoCreateTable.Load() {
		return nil
	}

	if _, exist := d.createdTables.Load(opt.TableName); exist {
		return nil
	}

	return d.CreateTable(v, opt)
}

func mergeOptions(v interface{}, opts ...Options) Options {
	opt := Options{}
	if intf, ok := v.(optionInterface); ok {
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
	if nil == opt.Indexes {
		opt.Indexes = make([]Index, 0)
	}
	if nil == opt.Uniques {
		opt.Uniques = make([]Unique, 0)
	}
	return opt
}

func (d *DB) mysqlCreateTable(fields []*Field, opt *Options) error {
	// create table
	var autoIncField *Field
	statements := make([]string, 0, len(fields)+len(opt.Indexes)+len(opt.Uniques)+1)

	// make fields statements
	for _, f := range fields {
		comment := strings.Replace(f.Comment, "'", "\\'", -1)
		null := ""
		if false == f.Nullable {
			null = "NOT NULL"
		}
		if f.AutoIncrement {
			autoIncField = f
			f.statement = fmt.Sprintf("`%s` %s %s AUTO_INCREMENT COMMENT '%s'", f.Name, f.Type, null, comment)
		} else {
			f.statement = fmt.Sprintf("`%s` %s %s DEFAULT %s COMMENT '%s'", f.Name, f.Type, null, f.Default, comment)
		}

		// log.Printf("statement: %s\n", f.statement)
		statements = append(statements, f.statement)
	}

	// make index statements
	if autoIncField != nil {
		s := fmt.Sprintf("PRIMARY KEY (`%s`)", autoIncField.Name)
		statements = append(statements, s)
		// log.Printf("statement: %s\n", s)
	}
	for _, idx := range opt.Indexes {
		if err := idx.Check(); err != nil {
			return err
		}

		idxFieldList := make([]string, 0, len(idx.Fields))
		for _, f := range idx.Fields {
			idxFieldList = append(idxFieldList, "`"+f+"`")
		}
		s := fmt.Sprintf("KEY `%s` (%s)", idx.Name, strings.Join(idxFieldList, ", "))
		statements = append(statements, s)
		// log.Printf("statememt: %s\n", s)
	}

	// make unique statements
	for _, uniq := range opt.Uniques {
		if err := uniq.Check(); err != nil {
			return err
		}

		uniqFieldList := make([]string, 0, len(uniq.Fields))
		for _, f := range uniq.Fields {
			uniqFieldList = append(uniqFieldList, "`"+f+"`")
		}

		s := fmt.Sprintf("UNIQUE KEY `%s` (%s)", uniq.Name, strings.Join(uniqFieldList, ", "))

		statements = append(statements, s)
		// log.Printf("statememt: %s\n", s)
	}

	// package final create statements
	desc := strings.Replace(opt.TableDescption, "'", "\\'", -1)
	autoIncOne := "AUTO_INCREMENT=1"
	if nil == autoIncField {
		autoIncOne = ""
	}
	final := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) ENGINE=InnoDB %s DEFAULT CHARSET=utf8 COMMENT '%s'",
		opt.TableName,
		strings.Join(statements, ",\n"),
		autoIncOne, desc,
	)

	// exec
	// log.Println(final)
	_, err := d.db.Exec(final)
	if err != nil {
		return err
	}

	return nil
}

func (d *DB) mysqlAlterTableFields(fields []*Field, fieldsInDB []*Field, opt *Options) error {
	prevFieldMap := make(map[string]*Field) // used for AFTER section in ALTER statements
	var prevField *Field
	for _, f := range fields {
		prevFieldMap[f.Name] = prevField
		prevField = f
	}

	// find missing columns and then alter them
	fieldsInDBMap := convFieldListToMap(fieldsInDB)
	for _, f := range fields {
		_, exist := fieldsInDBMap[f.Name]
		if exist {
			continue
		}
		// new primary key is not allowed
		if f.AutoIncrement {
			return fmt.Errorf("new promary key `%s` (auto increment) is not allowed", f.Name)
		}

		// not exist? should ALTER
		var funcInsertField func(*Field) error
		funcInsertField = func(f *Field) error {
			var statement string
			comment := strings.Replace(f.Comment, "'", "\\'", -1)
			null := ""
			if false == f.Nullable {
				null = "NOT NULL"
			}

			prevField, _ := prevFieldMap[f.Name]
			if prevField == nil {
				// this is first column
				statement = fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' FIRST", opt.TableName, f.Name, f.Type, null, f.Default, comment)

			} else {
				_, prevExistsInDB := fieldsInDBMap[prevField.Name]
				if false == prevExistsInDB {
					// previous map has not been inserted
					err := funcInsertField(prevField)
					if err != nil {
						return err
					}
				}

				statement = fmt.Sprintf("ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' AFTER `%s`", opt.TableName, f.Name, f.Type, null, f.Default, comment, prevField.Name)
			}

			// log.Println(statement)
			_, err := d.db.Exec(statement)
			if err != nil {
				return err
			}

			fieldsInDBMap[f.Name] = f
			return nil
		}

		err := funcInsertField(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *DB) mysqlAlterTableIndexUniques(opt *Options) error {
	// read index and uniques
	indexInDB, uniqInDB, err := d.ReadTableIndexes(opt.TableName)
	if err != nil {
		return err
	}

	// add indexes
	for _, idx := range opt.Indexes {
		if err := idx.Check(); err != nil {
			return err
		}

		if _, exist := indexInDB[idx.Name]; exist {
			continue
		}

		idxFieldList := make([]string, 0, len(idx.Fields))
		for _, f := range idx.Fields {
			idxFieldList = append(idxFieldList, "`"+f+"`")
		}
		s := fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", opt.TableName, idx.Name, strings.Join(idxFieldList, ", "))

		_, err := d.db.Exec(s)
		if err != nil {
			return err
		}
	}

	// add uniques
	for _, uniq := range opt.Uniques {
		if err := uniq.Check(); err != nil {
			return err
		}

		if _, exist := uniqInDB[uniq.Name]; exist {
			continue
		}

		uniqFieldList := make([]string, 0, len(uniq.Fields))
		for _, f := range uniq.Fields {
			uniqFieldList = append(uniqFieldList, "`"+f+"`")
		}
		s := fmt.Sprintf("ALTER TABLE `%s` ADD UNIQUE `%s` (%s)", opt.TableName, uniq.Name, strings.Join(uniqFieldList, ", "))

		_, err := d.db.Exec(s)
		if err != nil {
			return err
		}
	}

	return nil
}

// CreateTable creates a table if not exist. If the table exists, it will alter it if necessary
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

	// read fields and check if table exists
	shouldCreate := false
	fieldsInDB, err := d.ReadTableFields(opt.TableName)
	if err != nil {
		if false == strings.Contains(err.Error(), "doesn't exist") {
			return err
		}

		shouldCreate = true
		// and then continue
	}

	// create or alter fields
	if shouldCreate || nil == fieldsInDB {
		return d.mysqlCreateTable(fields, &opt)
	}

	// check and alter fields
	err = d.mysqlAlterTableFields(fields, fieldsInDB, &opt)
	if err != nil {
		return err
	}
	// check and alter indexes and uniques
	err = d.mysqlAlterTableIndexUniques(&opt)
	if err != nil {
		return err
	}

	d.createdTables.Store(opt.TableName, true)
	return err
}

var _defaultIntegerTypes = map[string]string{
	"int":    "int",
	"uint":   "int unsigned",
	"int64":  "bigint",
	"uint64": "bigint unsigned",
	"int32":  "int",
	"uint32": "int unsigned",
	"int16":  "smallint",
	"uint16": "smallint unsigned",
	"int8":   "tinyint",
	"uint8":  "tinyint unsigned",
}

func readStructFields(t reflect.Type, v reflect.Value) (ret []*Field, err error) {
	numField := t.NumField()
	ret = make([]*Field, 0, numField)

	for i := 0; i < numField; i++ {
		tf := t.Field(i) // *StructField
		vf := v.Field(i) // *Value
		if false == vf.CanInterface() {
			// log.Println(tf.Type, " cannot interface")
			continue
		}

		fieldName := getFieldName(&tf)
		fieldType := ""
		fieldNull := false
		fieldDflt := ""
		fieldIncr := false
		fieldComt := ""

		if fieldName == "" || fieldName == "-" {
			if tf.Type.Kind() != reflect.Struct {
				continue // skip this
			}
		}

		switch vf.Interface().(type) {
		case int, uint, int64, uint64, int32, uint32, int16, uint16, int8, uint8:
			fieldTypeName := reflect.TypeOf(vf.Interface()).String()
			fieldType = getFieldType(&tf, _defaultIntegerTypes[fieldTypeName])
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _Integer, fieldNull)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		case bool:
			fieldType = getFieldType(&tf, "boolean")
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _Bool, fieldNull)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		case string:
			fieldType = getFieldType(&tf, "")
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _String, fieldNull)
			fieldIncr = false
			fieldComt = getFieldComment(&tf)
			if "" == fieldType {
				return nil, fmt.Errorf("missing type tag for string field '%s'", fieldName)
			}
		case float32, float64:
			fieldType = getFieldType(&tf, "")
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _Float, fieldNull)
			fieldIncr = false
			fieldComt = getFieldComment(&tf)
			if "" == fieldType {
				return nil, fmt.Errorf("missing type tag for float field '%s'", fieldName)
			}
		case sql.NullString:
			fieldType = getFieldType(&tf, "")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _String, fieldNull)
			fieldIncr = false
			fieldComt = getFieldComment(&tf)
			if "" == fieldType {
				return nil, fmt.Errorf("missing type tag for sql.NullString field '%s'", fieldName)
			}
		case sql.NullInt64:
			fieldType = getFieldType(&tf, "bigint")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _Integer, fieldNull)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		case sql.NullBool:
			fieldType = getFieldType(&tf, "boolean")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _Bool, fieldNull)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		case sql.NullFloat64:
			fieldType = getFieldType(&tf, "")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _Float, fieldNull)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
			if "" == fieldType {
				return nil, fmt.Errorf("missing type tag for sql.NullFloat64 field '%s'", fieldName)
			}
		case mysql.NullTime:
			fieldType = getFieldType(&tf, "datetime")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _DateTime, fieldNull, fieldType)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		case time.Time:
			fieldType = getFieldType(&tf, "datetime")
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _DateTime, fieldNull, fieldType)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
		default:
			if tf.Type.Kind() == reflect.Struct {
				// log.Println("Embedded struct: ", tf.Type)
				subFields, err := readStructFields(tf.Type, vf)
				if err != nil {
					return nil, err
				}
				ret = append(ret, subFields...)
			}
			continue
		}

		// done
		ret = append(ret, &Field{
			Name:          fieldName,
			Type:          fieldType,
			Nullable:      fieldNull,
			Default:       fieldDflt,
			AutoIncrement: fieldIncr,
			Comment:       fieldComt,
		})
	}
	return
}

func getFieldName(tf *reflect.StructField) string {
	dbTagList := strings.SplitN(tf.Tag.Get("db"), ",", 2)
	if nil == dbTagList || 0 == len(dbTagList) || "" == dbTagList[0] {
		// no db tags, this field would be ignored
		// return _fieldNameToSQL(tf.Name)
		return ""
	}

	return dbTagList[0]
}

func getFieldType(tf *reflect.StructField, dft string) string {
	t := _readMysqlxTag(tf, "type")
	if "" == t {
		return dft
	}
	if 'u' == t[0] {
		return t[1:] + " unsigned"
	}
	return t
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
		case _String:
			return "'" + strings.Replace(n, "'", "\\'", -1) + "'"
		case _DateTime:
			if n == "0" {
				return "0"
			}
			return "'" + strings.Replace(n, "_", " ", -1) + "'"
		default:
			return n
		}

	} else {
		if nullable {
			return "NULL"
		}
		switch category {
		case _String:
			return "''"
		case _Integer, _Float:
			return "0"
		case _Bool:
			return "FALSE"
		case _DateTime:
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
	fullTag := tf.Tag.Get("comment")
	return strings.Trim(fullTag, " \t")
}

// tools only for this file
func _fieldNameToSQL(field string) string {
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
	fullTag := tf.Tag.Get("mysqlx")
	kvStrParts := strings.Split(fullTag, " ")

	// search for the key
	for _, s := range kvStrParts {
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
