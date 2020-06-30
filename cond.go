package mysqlx

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Cond is for constructing MySQL WHERE statement
type Cond struct {
	Param    string
	Operator string
	Value    interface{}
}

// Condition return Cond data in function format
func Condition(param, operator string, value interface{}) *Cond {
	return &Cond{
		Param:    param,
		Operator: operator,
		Value:    value,
	}
}

// Like return Cond data with LIKE operator in function format
func Like(param string, likeParts []string) *Cond {
	if 0 == len(likeParts) {
		return nil
	}

	c := &Cond{
		Param:    param,
		Operator: "LIKE",
	}

	parts := make([]string, 0, len(likeParts))
	for _, p := range likeParts {
		parts = append(parts, escapeLikeValueString(p))
	}

	c.Value = strings.Join(parts, "%")
	return c
}

// parseCondIn is invoked by parseCond. This handles sutiations those the operator is "in".
func (c *Cond) parseIn(fieldMap map[string]*Field) (field, operator, value string, err error) {
	operator = "IN"
	field = c.Param

	switch c.Value.(type) {
	default:
		err = fmt.Errorf("invalid condition value type following by '%s'", c.Operator)
		return
	case []int, []uint, []int8, []uint8, []int16, []uint16, []int32, []uint32, []int64, []uint64, []float32, []float64:
		s := fmt.Sprintf("%+v", c.Value)
		s = strings.Replace(s, " ", ", ", -1)
		s = strings.Replace(s, "[", "(", -1)
		s = strings.Replace(s, "]", ")", -1)
		value = s
	case []string:
		in := c.Value.([]string)
		out := make([]string, 0, len(in))
		for _, s := range in {
			out = append(out, addQuoteToString(s, "'"))
		}
		value = "(" + strings.Join(out, ", ") + ")"
	case []time.Time:
		in := c.Value.([]time.Time)
		_, exist := fieldMap[c.Param]
		if false == exist {
			err = fmt.Errorf("field '%s' not found", c.Param)
			return
		}
		values := make([]string, 0, len(in))
		for _, t := range in {
			values = append(values, convTimeToString(t, fieldMap, c.Param))
		}
		value = "(" + strings.Join(values, ", ") + ")"
	}

	if value == "()" {
		err = fmt.Errorf("empty values in field '%s'", field)
		return
	}
	return
}

func (c *Cond) parse(fieldMap map[string]*Field) (field, operator, value string, err error) {
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
	case "=", "!=", "<>", ">", "<", ">=", "<=":
		// continue below
	case "IS", "is":
		c.Operator = "IS"
	case "IS NOT", "is not", "not", "NOT":
		c.Operator = "IS NOT"
	case "in", "IN":
		return c.parseIn(fieldMap)
	case "not in", "NOT IN":
		field, _, value, err = c.parseIn(fieldMap)
		operator = "NOT IN"
		return
	case "LIKE", "like":
		v, ok := c.Value.(string)
		if false == ok {
			err = fmt.Errorf("LIKE value should be raw string")
			return
		}
		field = c.Param
		operator = "LIKE"
		value = "'" + v + "'"
		return
	default:
		err = fmt.Errorf("invalid operator '%s'", c.Operator)
		return
	}

	// value
	switch c.Value.(type) {
	default:
		err = fmt.Errorf("invalid condition value type following by '%s'", c.Operator)
		return
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
		s = escapeValueString(s)
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

func (c *Cond) pack(fieldMap map[string]*Field) string {
	field, operator, value, err := c.parse(fieldMap)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("`%s` %s %s", field, operator, value)
}
