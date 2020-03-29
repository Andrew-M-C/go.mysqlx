// Package structs is a simple test package for mysqlx. Please do not use this.
package structs

import (
	"log"
	"reflect"
)

var (
	debugf = log.Printf
)

// CopyStruct looks into a structure or structure pointer, and returns a copy
// of the original one. Returned value will ALWAYS be a pointer to a structure.
// If the input parameter is not a structure or pointer to a structure, this
// function returns nil.
func CopyStruct(s interface{}) (ptr interface{}) {
	isStruct, isPtr := checkInterfaceStructOrPointer(s)
	if isStruct {
		val := reflect.ValueOf(s)
		return copyFromStructVal(val)
	}
	if isPtr {
		val := reflect.ValueOf(s).Elem()
		return copyFromStructVal(val)
	}
	return nil
}

func checkInterfaceStructOrPointer(
	intf interface{},
) (isStruct, isPtrToStruct bool) {
	if nil == intf {
		return
	}

	ty := reflect.TypeOf(intf)
	switch ty.Kind() {
	case reflect.Struct:
		isStruct = true
		return
	case reflect.Ptr:
		// continue checking
	default:
		return
	}

	va := reflect.ValueOf(intf).Elem()
	isPtrToStruct = (va.Kind() == reflect.Struct)
	return
}

// reference: https://stackoverflow.com/questions/42664837/how-to-access-unexported-struct-fields-in-golang
func copyStructContents(from, to reflect.Value) {
	ty := from.Type()

	if ty.Kind() == reflect.Struct {
		fieldCount := ty.NumField()
		for i := 0; i < fieldCount; i++ {
			toValF := to.Field(i)
			fromValF := from.Field(i)
			copyStructContents(fromValF, toValF)
		}
		return
	}
	if to.CanSet() {
		to.Set(from)
		return
	}

	// to = reflect.NewAt(from.Type(), unsafe.Pointer(to.UnsafeAddr())).Elem()
	// to.Set(from)
	debugf("type %v cannot be set", from.Type())
	return
}

func copyFromStructVal(val reflect.Value) (res interface{}) {
	// ty := val.Type()
	// resVal := reflect.New(ty)
	// resElem := resVal.Elem()

	// copyStructContents(val, resElem)
	// return resVal.Interface()

	resVal := reflect.New(val.Type())
	resElem := resVal.Elem()
	resElem.Set(val)
	return resVal.Interface()
}
