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
func (d *xdb) MustCreateTable(v interface{}, opts ...Options) {
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
func (d *xdb) AutoCreateTable() {
	d.autoCreateTable.Store(true)
}

func (d *xdb) checkAutoCreateTable(v interface{}, opt Options) error {
	if false == d.autoCreateTable.Load() {
		return nil
	}

	if _, exist := d.createdTables.Load(opt.TableName); exist {
		return nil
	}

	return d.CreateTable(v, opt)
}

// duplicateStructAndGetOpts As we cannot directly get the address value
// from a structure reflect.Value, what we could do is duplicate it and
// create a new pointer type and invoke optionInterface from it.
// Reference: https://groups.google.com/forum/#!topic/golang-nuts/KB3_Yj3Ny4c
func duplicateStructAndGetOpts(v interface{}) Options {
	// v is a structure here
	val := reflect.ValueOf(v)
	newVal := reflect.New(val.Type())
	elem := newVal.Elem()
	elem.Set(val)

	v = newVal.Interface()
	if intf, ok := v.(optionInterface); ok {
		return intf.Options()
	}

	return Options{}
}

func mergeOptions(v interface{}, opts ...Options) Options {
	// v is a structure type here
	opt := Options{}
	if intf, ok := v.(optionInterface); ok {
		opt = intf.Options()
	} else {
		opt = duplicateStructAndGetOpts(v)
	}

	if nil == opt.CreateTableParams {
		opt.CreateTableParams = map[string]string{}
	}

	if len(opts) > 0 {
		// copy each option
		if "" != opts[0].TableName {
			opt.TableName = opts[0].TableName
		}
		if "" != opts[0].TableDescption {
			opt.TableDescption = opts[0].TableDescption
		}
		if len(opts[0].Indexes) > 0 {
			opt.Indexes = opts[0].Indexes
		}
		if len(opts[0].Uniques) > 0 {
			opt.Uniques = opts[0].Uniques
		}
		if len(opts[0].CreateTableParams) > 0 {
			if nil == opt.CreateTableParams {
				opt.CreateTableParams = opts[0].CreateTableParams
			} else {
				for k, v := range opts[0].CreateTableParams {
					opt.CreateTableParams[k] = v
				}
			}
		}
		opt.DoNotExec = opts[0].DoNotExec
	}
	if nil == opt.Indexes {
		opt.Indexes = make([]Index, 0)
	}
	if nil == opt.Uniques {
		opt.Uniques = make([]Unique, 0)
	}
	return opt
}

func (d *xdb) mysqlCreateTableStatement(fields []*Field, opt *Options) (string, error) {
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
		onUpdate := ""
		if f.OnUpdate != "" {
			onUpdate = fmt.Sprintf("ON UPDATE %s", f.OnUpdate)
		}
		if f.AutoIncrement {
			autoIncField = f
			f.statement = fmt.Sprintf("`%s` %s %s AUTO_INCREMENT COMMENT '%s'", f.Name, f.Type, null, comment)
		} else {
			f.statement = fmt.Sprintf("`%s` %s %s DEFAULT %s %s COMMENT '%s'", f.Name, f.Type, null, f.Default, onUpdate, comment)
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
			return "", err
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
			return "", err
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
	extOptions := map[string]string{
		"ENGINE":          "InnoDB",
		"DEFAULT CHARSET": "utf8mb4",
	}

	if autoIncField != nil {
		extOptions["AUTO_INCREMENT"] = "1"
	}

	// customized options
	for k, v := range opt.CreateTableParams {
		k = strings.ToUpper(k)
		extOptions[k] = v
	}

	// additional build options
	options := bytes.Buffer{}
	for k, v := range extOptions {
		options.WriteString(k)
		options.WriteRune('=')
		options.WriteString(v)
		options.WriteRune(' ')
	}

	// refernece: https://www.cnblogs.com/ilovewindy/p/4726786.html
	final := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (\n%s\n) %sCOMMENT '%s'",
		opt.TableName,
		strings.Join(statements, ",\n"),
		options.String(),
		desc,
	)

	// done
	// log.Println(final)
	return final, nil
}

func (d *xdb) mysqlAlterTableFieldsStatements(fields []*Field, fieldsInDB []*Field, opt *Options) (ret []string, err error) {
	ret = []string{}
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
			return nil, fmt.Errorf("new promary key `%s` (auto increment) is not allowed", f.Name)
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
				if f.OnUpdate == "" {
					statement = fmt.Sprintf(
						"ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' FIRST",
						opt.TableName, f.Name, f.Type, null, f.Default, comment,
					)
				} else {
					statement = fmt.Sprintf(
						"ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s ON UPDATE %s COMMENT '%s' FIRST",
						opt.TableName, f.Name, f.Type, null, f.Default, f.OnUpdate, comment,
					)
				}

			} else {
				_, prevExistsInDB := fieldsInDBMap[prevField.Name]
				if false == prevExistsInDB {
					// previous map has not been inserted
					err := funcInsertField(prevField)
					if err != nil {
						return err
					}
				}

				if f.OnUpdate == "" {
					statement = fmt.Sprintf(
						"ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s COMMENT '%s' AFTER `%s`",
						opt.TableName, f.Name, f.Type, null, f.Default, comment, prevField.Name,
					)
				} else {
					statement = fmt.Sprintf(
						"ALTER TABLE `%s` ADD COLUMN `%s` %s %s DEFAULT %s ON UPDATE %s COMMENT '%s' AFTER `%s`",
						opt.TableName, f.Name, f.Type, null, f.Default, f.OnUpdate, comment, prevField.Name,
					)
				}

			}

			// log.Println(statement)
			ret = append(ret, statement)
			fieldsInDBMap[f.Name] = f
			return nil
		}

		err = funcInsertField(f)
		if err != nil {
			return
		}
	}

	return ret, nil
}

func (d *xdb) mysqlAlterTableIndexUniquesStatements(opt *Options) (ret []string, err error) {
	ret = []string{}
	// read index and uniques
	indexInDB, uniqInDB, err := d.ReadTableIndexes(opt.TableName)
	if err != nil {
		return
	}

	// add indexes
	for _, idx := range opt.Indexes {
		if err = idx.Check(); err != nil {
			return
		}

		if _, exist := indexInDB[idx.Name]; exist {
			continue
		}

		idxFieldList := make([]string, 0, len(idx.Fields))
		for _, f := range idx.Fields {
			idxFieldList = append(idxFieldList, "`"+f+"`")
		}
		s := fmt.Sprintf("ALTER TABLE `%s` ADD INDEX `%s` (%s)", opt.TableName, idx.Name, strings.Join(idxFieldList, ", "))
		ret = append(ret, s)
	}

	// add uniques
	for _, uniq := range opt.Uniques {
		if err = uniq.Check(); err != nil {
			return
		}

		if _, exist := uniqInDB[uniq.Name]; exist {
			continue
		}

		uniqFieldList := make([]string, 0, len(uniq.Fields))
		for _, f := range uniq.Fields {
			uniqFieldList = append(uniqFieldList, "`"+f+"`")
		}
		s := fmt.Sprintf("ALTER TABLE `%s` ADD UNIQUE `%s` (%s)", opt.TableName, uniq.Name, strings.Join(uniqFieldList, ", "))
		ret = append(ret, s)
	}

	return ret, nil
}

// CreateOrAlterTableStatements returns 'CREATE TABLE ... IF NOT EXISTS ...' or 'ALTER TABLE ...' statements, but will not execute them.
// If the table does not exists, 'CREATE TABLE ...' statement will be returned. If the table exists and needs no alteration, an empty
// string slice would be returned. Otherwise, a string slice with 'ALTER TABLE ...' statements would be returned.
//
// The returned exists identifies if the table exists in database.
func (d *xdb) CreateOrAlterTableStatements(v interface{}, opts ...Options) (exists bool, statements []string, err error) {
	exists, create, alter, _, err := d.createAndAlterTableStatements(v, opts...)
	if err != nil {
		return
	}
	if "" != create {
		statements = []string{create}
	} else {
		statements = alter
	}
	return
}

func (d *xdb) createAndAlterTableStatements(v interface{}, opts ...Options) (exists bool, create string, alter []string, opt Options, err error) {
	if nil == d.db {
		err = fmt.Errorf("mysqlx not initialized")
		return
	}

	// read options
	alter = []string{}
	opt = mergeOptions(v, opts...)
	// log.Printf("final opts: %+v", opt)

	// check options
	if "" == opt.TableName {
		err = fmt.Errorf("table name not specified")
		return
	}

	// read fields
	// log.Println("now start ReadStructFields")
	fields, err := ReadStructFields(v)
	if err != nil {
		return
	}

	// read fields and check if table exists
	shouldCreate := false
	// log.Println("now start readTableFields")
	fieldsInDB, err := d.ReadTableFields(opt.TableName)
	if err != nil {
		if false == strings.Contains(err.Error(), "doesn't exist") {
			return
		}
		shouldCreate = true
		// and then continue
	}
	if nil == fieldsInDB || 0 == len(fieldsInDB) {
		shouldCreate = true
	}

	// create or alter fields
	if shouldCreate || nil == fieldsInDB {
		// log.Println("shouldCreate")
		create, err = d.mysqlCreateTableStatement(fields, &opt)
		return
	}
	exists = true

	// check and alter fields
	alterFieldStatements, err := d.mysqlAlterTableFieldsStatements(fields, fieldsInDB, &opt)
	if err != nil {
		return
	}
	// check and alter indexes and uniques
	alterIndexStatements, err := d.mysqlAlterTableIndexUniquesStatements(&opt)
	if err != nil {
		return
	}

	alter = append(alterFieldStatements, alterIndexStatements...)
	return
}

// CreateTable creates a table if not exist. If the table exists, it will alter it if necessary
func (d *xdb) CreateTable(v interface{}, opts ...Options) error {
	exists, create, alter, opt, err := d.createAndAlterTableStatements(v, opts...)
	if err != nil {
		return err
	}

	// if table not exists
	if false == exists {
		if opt.DoNotExec {
			return newError(doNotExec, create)
		}
		_, err = d.db.Exec(create)
		if err != nil {
			return newError(err.Error(), create)
		}

		d.createdTables.Store(opt.TableName, true)
		return nil
	}

	// alter
	if opt.DoNotExec {
		return newError(doNotExec, strings.Join(alter, ";\n"))
	}
	for _, query := range alter {
		_, err = d.db.Exec(query)
		if err != nil {
			return newError(err.Error(), strings.Join(alter, ";\n"))
		}
	}

	d.createdTables.Store(opt.TableName, true)
	return nil
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
		fieldOnUpdate := ""

		if fieldName == "-" {
			continue
		}
		if fieldName == "" {
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
			fieldOnUpdate = getFieldOnUpdate(&tf)
		case sql.NullTime:
			fieldType = getFieldType(&tf, "datetime")
			fieldNull = getFieldNullable(&tf, true)
			fieldDflt = getFieldDefault(&tf, _DateTime, fieldNull, fieldType)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
			fieldOnUpdate = getFieldOnUpdate(&tf)
		case time.Time:
			fieldType = getFieldType(&tf, "datetime")
			fieldNull = getFieldNullable(&tf, false)
			fieldDflt = getFieldDefault(&tf, _DateTime, fieldNull, fieldType)
			fieldIncr = getFieldAutoIncrement(&tf, false)
			fieldComt = getFieldComment(&tf)
			fieldOnUpdate = getFieldOnUpdate(&tf)
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
			OnUpdate:      fieldOnUpdate,
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

func getFieldOnUpdate(tf *reflect.StructField) string {
	n := _readMysqlxTag(tf, "onupdate")
	return n
}

func getFieldDefault(tf *reflect.StructField, category category, nullable bool, fieldTypes ...string) string {
	n := _readMysqlxTag(tf, "default")
	if n != "" {
		switch category {
		case _String:
			return "'" + strings.Replace(n, "'", "\\'", -1) + "'"
		case _DateTime:
			if strings.Contains(strings.ToUpper(n), "TIMESTAMP") {
				return n
			}
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
