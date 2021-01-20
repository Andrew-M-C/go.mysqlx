package mysqlx

import (
	"fmt"
	"reflect"
)

// This file handles argument parsing

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
		c := ""
		o := ""
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
			ret.Limit = int(arg.(Limit))
		case Offset:
			ret.Offset = int(arg.(Offset))
		case Cond:
			cond := arg.(Cond)
			c = cond.pack(ret.FieldMap)
		case *Cond:
			cond := arg.(*Cond)
			c = cond.pack(ret.FieldMap)
		case And:
			and := arg.(And)
			c = and.pack(ret.FieldMap)
		case *And:
			and := arg.(*And)
			c = and.pack(ret.FieldMap)
		case Or:
			or := arg.(Or)
			c = or.pack(ret.FieldMap)
		case *Or:
			or := arg.(*Or)
			c = or.pack(ret.FieldMap)
		case Order:
			order := arg.(Order)
			o = order.pack()
		case *Order:
			order := arg.(*Order)
			o = order.pack()
		}

		if "" != c {
			ret.CondList = append(ret.CondList, c)
		}
		if "" != o {
			ret.OrderList = append(ret.OrderList, o)
		}
	}

	if "" == ret.Opt.TableName {
		err = fmt.Errorf("nil table name")
		return
	}
	return
}
