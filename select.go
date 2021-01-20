package mysqlx

import (
	"fmt"
	"reflect"
	"strings"
)

// ========

// SelectFields returns all valid SQL fields in given structure
func (d *xdb) SelectFields(s interface{}) (string, error) {
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

func (d *xdb) getFieldMap(prototype interface{}) (fieldMap map[string]*Field, err error) {
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

func (d *xdb) getIncrementField(prototype interface{}) (field *Field, err error) {
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

// Select execute a SQL select statement
func (d *xdb) Select(dst interface{}, args ...interface{}) error {
	return d.selectFunc(d.db, dst, args...)
}

func (d *xdb) selectFunc(obj sqlObj, dst interface{}, args ...interface{}) error {

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
	// log.Println("select query:", query)
	if parsedArgs.Opt.DoNotExec {
		return newError(doNotExec, query)
	}

	err = obj.Select(dst, query)
	if err != nil {
		err = newError(err.Error(), query)
		return err
	}
	return err
}
