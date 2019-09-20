package mysqlx

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
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
