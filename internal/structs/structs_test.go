package structs

import (
	"testing"
)

func TestTypeLooking(t *testing.T) {
	printf := t.Logf

	isStruct, isPtr := checkInterfaceStructOrPointer(struct{}{})
	printf("%v - %v", isStruct, isPtr)

	isStruct, isPtr = checkInterfaceStructOrPointer(&struct{}{})
	printf("%v - %v", isStruct, isPtr)

	isStruct, isPtr = checkInterfaceStructOrPointer(*(&struct{}{}))
	printf("%v - %v", isStruct, isPtr)

	isStruct, isPtr = checkInterfaceStructOrPointer(nil)
	printf("%v - %v", isStruct, isPtr)

	isStruct, isPtr = checkInterfaceStructOrPointer([]struct{}{})
	printf("%v - %v", isStruct, isPtr)

	isStruct, isPtr = checkInterfaceStructOrPointer(123)
	printf("%v - %v", isStruct, isPtr)

	return
}

type Struct struct {
	String  string
	_string string
}

type Various struct {
	Int       int
	Bool      bool
	String    string
	Float     float32
	Array     [8]int
	Slice     []int
	Struct    Struct
	StructPtr *Struct

	_int       int
	_string    string
	_struct    Struct
	_structPtr *Struct
}

func TestStructCopy(t *testing.T) {
	printf := t.Logf
	structA := Struct{
		String:  "structA",
		_string: "_structA",
	}
	structB := Struct{
		String:  "structB",
		_string: "_structB",
	}
	internalA := Struct{
		String:  "internalA",
		_string: "_internalA",
	}
	internalB := Struct{
		String:  "internalB",
		_string: "_internalB",
	}

	from := Various{
		Int:       1,
		Bool:      true,
		String:    "various",
		Float:     2.2,
		Array:     [8]int{1, 2, 3, 4, 5, 6, 7, 8},
		Slice:     []int{11, 22, 33, 44},
		Struct:    structA,
		StructPtr: &structB,

		_int:       11,
		_string:    "private",
		_struct:    internalA,
		_structPtr: &internalB,
	}

	to := CopyStruct(from)

	printf("orig:\n&%+v", from)
	printf("copied:\n%+v", to)

	return
}
