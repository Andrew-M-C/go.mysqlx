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

// SelectFields returns all valid SQL fields in given structure
func (d *DB) SelectFields(s interface{}) (string, error) {
	// TODO: read interface until we get a struct

	// read from buffer
	intfName := reflect.TypeOf(s)
	// log.Printf("select type %v", intfName)
	fieldValue, exist := d.bufferedSelectFields.Load(intfName)
	if exist {
		return fieldValue.(string), nil
	}

	fields, err := d.ReadStructFields(s)
	if err != nil {
		return "", err
	}

	fieldNames := make([]string, 0, len(fields))
	for _, f := range fields {
		fieldNames = append(fieldNames, "`"+f.Name+"`")
	}

	ret := strings.Join(fieldNames, ", ")
	d.bufferedSelectFields.Store(intfName, ret)
	return ret, nil
}

func (d *DB) getFieldMap(prototype interface{}) (fieldMap map[string]*Field, err error) {
	intfName := reflect.TypeOf(prototype)
	if fieldMapValue, exist := d.bufferedFieldMaps.Load(intfName); exist {
		fieldMap = fieldMapValue.(map[string]*Field)
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
		d.bufferedFieldMaps.Store(intfName, fieldMap)
	}
	return
}

func (d *DB) getIncrementField(prototype interface{}) (field *Field, err error) {
	intfName := reflect.TypeOf(prototype)
	if fieldValue, exist := d.bufferedIncrField.Load(intfName); exist {
		field = fieldValue.(*Field)
		return
	}

	var fields []*Field
	fields, err = d.ReadStructFields(prototype)
	if err != nil {
		return
	}

	for _, f := range fields {
		if f.AutoIncrement {
			d.bufferedIncrField.Store(intfName, f)
			return f, nil
		}
	}

	return nil, fmt.Errorf("'%s' has no increment field", intfName)
}

type _parsedArgs struct {
	FieldMap  map[string]*Field
	Opt       Options
	Offset    int
	Limit     int
	CondList  []string
	OrderList []string
}

func (d *DB) handleArgs(prototype interface{}, args []interface{}) (ret *_parsedArgs, err error) {
	ret = &_parsedArgs{
		CondList:  make([]string, 0, len(args)),
		OrderList: make([]string, 0, len(args)),
	}

	ret.FieldMap, err = d.getFieldMap(prototype)
	if err != nil {
		return
	}
	ret.Opt = mergeOptions(prototype)

	for _, arg := range args {
		switch arg.(type) {
		default:
			t := reflect.TypeOf(arg)
			err = fmt.Errorf("unsupported type %v", t)
			return
		case *Options:
			ret.Opt = mergeOptions(prototype, *(arg.(*Options)))
		case Options:
			ret.Opt = mergeOptions(prototype, arg.(Options))
		case Limit:
			ret.Limit = arg.(Limit).Limit
		case *Limit:
			ret.Limit = arg.(*Limit).Limit
		case Offset:
			ret.Offset = arg.(Offset).Offset
		case *Offset:
			ret.Offset = arg.(*Offset).Offset
		case Cond:
			cond := arg.(Cond)
			c := packCond(&cond, ret.FieldMap)
			if "" != c {
				ret.CondList = append(ret.CondList, c)
			}
		case *Cond:
			cond := arg.(*Cond)
			c := packCond(cond, ret.FieldMap)
			if "" != c {
				ret.CondList = append(ret.CondList, c)
			}
		case Order:
			order := arg.(Order)
			o := packOrder(&order)
			if "" != o {
				ret.OrderList = append(ret.OrderList, o)
			}
		case *Order:
			order := arg.(*Order)
			o := packOrder(order)
			if "" != o {
				ret.OrderList = append(ret.OrderList, o)
			}
		}
	}

	if "" == ret.Opt.TableName {
		err = fmt.Errorf("nil table name")
		return
	}
	return
}

// Select execute a SQL select statement
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
	fieldsStr, err := d.SelectFields(prototype)
	if err != nil {
		// log.Printf("read fields failed: %v", err)
		return err
	}

	// parse arguments
	parsedArgs, err := d.handleArgs(prototype, args)
	if err != nil {
		return err
	}

	// pack SELECT statements
	var offsetStr string
	if parsedArgs.Offset > 0 {
		offsetStr = fmt.Sprintf("OFFSET %d", parsedArgs.Offset)
	}

	var limitStr string
	if parsedArgs.Limit > 0 {
		limitStr = fmt.Sprintf("LIMIT %d", parsedArgs.Limit)
	}

	var orderStr string
	if len(parsedArgs.OrderList) > 0 {
		orderStr = "ORDER BY " + strings.Join(parsedArgs.OrderList, ", ")
	}

	var condStr string
	if len(parsedArgs.CondList) > 0 {
		condStr = "WHERE " + strings.Join(parsedArgs.CondList, " AND ")
	}

	query := fmt.Sprintf(
		"SELECT %s FROM `%s` %s %s %s %s",
		fieldsStr, parsedArgs.Opt.TableName, condStr, orderStr, limitStr, offsetStr,
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

// InsertFields return keys and values for inserting. Auto-increment fields will be ignored
func (d *DB) InsertFields(s interface{}, backQuoted bool) (keys []string, values []string, err error) {
	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)

	// read from buffer
	fieldMap, err := d.getFieldMap(s)
	if err != nil {
		return
	}

	// handle each fields
	numField := t.NumField()
	keys = make([]string, 0, numField)
	values = make([]string, 0, numField)

	for i := 0; i < numField; i++ {
		tf := t.Field(i) // *StructField
		vf := v.Field(i) // *Value
		if false == vf.CanInterface() {
			// // log.Println(tf.Type, " cannot interface")
			continue
		}

		fieldName := getFieldName(&tf)
		if fieldName == "" || fieldName == "-" {
			if tf.Type.Kind() != reflect.Struct {
				continue // skip this
			}
		}

		var val string
		intf := vf.Interface()

		f, exist := fieldMap[fieldName]
		if false == exist || f.AutoIncrement {
			continue
		}

		switch intf.(type) {
		case int, int8, int16, int32, int64:
			val = strconv.FormatInt(vf.Int(), 10)
		case uint, uint8, uint16, uint32, uint64:
			val = strconv.FormatUint(vf.Uint(), 10)
		case string:
			val = addQuoteToString(vf.String(), "'")
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

// ========

// Update execute UPDATE SQL statement with given structure and conditions
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

	return d.db.Exec(query)
}

func (d *DB) genUpdateKVs(prototype interface{}, fields map[string]interface{}) ([]string, error) {
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
			s := v.(string)
			kv = append(kv, "`"+k+"`"+" = "+addQuoteToString(s, "'"))
		case time.Time:
			t := v.(time.Time)
			valStr := convTimeToString(t, fieldMap, k)
			kv = append(kv, "`"+k+"`"+" = "+valStr)
		case nil:
			kv = append(kv, "`"+k+"`"+" = NULL")
		}
	}
	return kv, nil
}

// ========

// Delete executes SQL DELETE statement with given conditions
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
	parsedArgs, err := d.handleArgs(prototype, args)
	if err != nil {
		return nil, err
	}

	// pack DELETE statements
	var limitStr string
	if parsedArgs.Limit > 0 {
		limitStr = fmt.Sprintf("LIMIT %d", parsedArgs.Limit)
	}

	var orderStr string
	if len(parsedArgs.OrderList) > 0 {
		orderStr = "ORDER BY " + strings.Join(parsedArgs.OrderList, ", ")
	}

	var condStr string
	if len(parsedArgs.CondList) > 0 {
		condStr = "WHERE " + strings.Join(parsedArgs.CondList, " AND ")
	}

	query := fmt.Sprintf(
		"DELETE FROM `%s` %s %s %s",
		parsedArgs.Opt.TableName, condStr, orderStr, limitStr,
	)
	// log.Println(query)

	return d.db.Exec(query)
}

// ========

// SelectOrInsert executes update-if-not-exists statement
func (d *DB) SelectOrInsert(insert interface{}, selectResult interface{}, conds ...interface{}) (res sql.Result, err error) {
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
	parsedArgs, err := d.handleArgs(insert, conds)
	if err != nil {
		// log.Printf("handleArgs() failed: %v", err)
		return nil, err
	}
	if 0 == len(parsedArgs.CondList) {
		return nil, fmt.Errorf("select conditions not given")
	}

	// should have increment field
	incrField, err := d.getIncrementField(insert)
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
	var firstList []string
	var secondList []string
	var thirdList []string

	firstList = make([]string, len(keys))
	secondList = make([]string, len(keys))
	thirdList = parsedArgs.CondList

	for i, k := range keys {
		v := values[i]
		firstList[i] = "`" + k + "`"
		secondList[i] = fmt.Sprintf("%s AS %s", v, addQuoteToString(k, "'"))
	}

	var randomField string
	for k := range parsedArgs.FieldMap {
		randomField = k
		break
	}
	query := fmt.Sprintf(
		"INSERT INTO `%s` (%s) SELECT * FROM (SELECT %s) AS tmp WHERE NOT EXISTS (SELECT `%s` FROM `%s` WHERE %s) LIMIT 1",
		parsedArgs.Opt.TableName, strings.Join(firstList, ", "), strings.Join(secondList, ", "), randomField, parsedArgs.Opt.TableName, strings.Join(thirdList, " AND "),
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
	selectFields, err := d.SelectFields(insert)
	if err != nil {
		return res, err
	}
	insertID, err := res.LastInsertId()
	if err != nil {
		// inserted, now select
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` = %d", selectFields, parsedArgs.Opt.TableName, incrField.Name, insertID)

	} else {
		// not inserted, just select as above
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE %s", selectFields, parsedArgs.Opt.TableName, strings.Join(parsedArgs.CondList, " AND "))
	}

	// log.Println(query)
	return res, d.db.Select(selectResult, query)
}

// ========

//InsertIfNotExists is the same as SelectOrInsert but lacking select statement
func (d *DB) InsertIfNotExists(insert interface{}, conds ...interface{}) (res sql.Result, err error) {
	return d.SelectOrInsert(insert, nil, conds...)
}
